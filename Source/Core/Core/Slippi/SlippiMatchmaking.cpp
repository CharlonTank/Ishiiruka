#include "SlippiMatchmaking.h"
#include "Common/Common.h"
#include "Common/CommonPaths.h"
#include "Common/ENetUtil.h"
#include "Common/FileUtil.h"
#include "Common/Logging/Log.h"
#include "Common/StringUtil.h"
#include <algorithm>
#include <string>
#include <vector>

#include "SlippiRustExtensions.h"

#if defined __linux__ && HAVE_ALSA
#elif defined __APPLE__
#include <arpa/inet.h>
#include <netdb.h>
#elif defined _WIN32
#endif

class MmMessageType
{
  public:
	static std::string CREATE_TICKET;
	static std::string CREATE_TICKET_RESP;
	static std::string GET_TICKET_RESP;
};

std::string MmMessageType::CREATE_TICKET = "create-ticket";
std::string MmMessageType::CREATE_TICKET_RESP = "create-ticket-resp";
std::string MmMessageType::GET_TICKET_RESP = "get-ticket-resp";

SlippiMatchmaking::SlippiMatchmaking(uintptr_t rs_exi_device_ptr, SlippiUser *user)
{
	m_user = user;
	m_state = ProcessState::IDLE;
	m_errorMsg = "";

	slprs_exi_device_ptr = rs_exi_device_ptr;

	m_client = nullptr;
	m_server = nullptr;

	MM_HOST = scm_slippi_semver_str.find("dev") == std::string::npos ? MM_HOST_PROD : MM_HOST_DEV;

	generator = std::default_random_engine(Common::Timer::GetTimeMs());
}

SlippiMatchmaking::~SlippiMatchmaking()
{
	isMmTerminated = true;
	m_state = ProcessState::ERROR_ENCOUNTERED;
	m_errorMsg = "Matchmaking shut down";

	if (m_matchmakeThread.joinable())
		m_matchmakeThread.join();

	terminateMmConnection();
}

void SlippiMatchmaking::FindMatch(MatchSearchSettings settings)
{
	isMmConnected = false;

	ERROR_LOG(SLIPPI_ONLINE, "[Matchmaking] Starting matchmaking...");

	m_searchSettings = settings;

	m_errorMsg = "";
	m_state = ProcessState::INITIALIZING;
	m_matchmakeThread = std::thread(&SlippiMatchmaking::MatchmakeThread, this);
}

SlippiMatchmaking::ProcessState SlippiMatchmaking::GetMatchmakeState()
{
	return m_state;
}

std::string SlippiMatchmaking::GetErrorMessage()
{
	return m_errorMsg;
}

bool SlippiMatchmaking::IsSearching()
{
	return searchingStates.count(m_state) != 0;
}

std::unique_ptr<SlippiNetplayClient> SlippiMatchmaking::GetNetplayClient()
{
	return std::move(m_netplayClient);
}

bool SlippiMatchmaking::IsFixedRulesMode(SlippiMatchmaking::OnlinePlayMode mode)
{
	return mode == SlippiMatchmaking::OnlinePlayMode::UNRANKED || mode == SlippiMatchmaking::OnlinePlayMode::RANKED ||
	       mode == SlippiMatchmaking::OnlinePlayMode::PARTY;
}

void SlippiMatchmaking::sendMessage(json msg)
{
	enet_uint32 flags = ENET_PACKET_FLAG_RELIABLE;
	u8 channelId = 0;

	std::string msgContents = msg.dump();

	ENetPacket *epac = enet_packet_create(msgContents.c_str(), msgContents.length(), flags);
	enet_peer_send(m_server, channelId, epac);
}

int SlippiMatchmaking::receiveMessage(json &msg, int timeoutMs)
{
	int hostServiceTimeoutMs = 250;

	// Make sure loop runs at least once
	if (timeoutMs < hostServiceTimeoutMs)
		timeoutMs = hostServiceTimeoutMs;

	// This is not a perfect way to timeout but hopefully it's close enough?
	int maxAttempts = timeoutMs / hostServiceTimeoutMs;

	for (int i = 0; i < maxAttempts; i++)
	{
		ENetEvent netEvent;
		int net = enet_host_service(m_client, &netEvent, hostServiceTimeoutMs);
		if (net <= 0)
			continue;

		switch (netEvent.type)
		{
		case ENET_EVENT_TYPE_RECEIVE:
		{

			std::vector<u8> buf;
			buf.insert(buf.end(), netEvent.packet->data, netEvent.packet->data + netEvent.packet->dataLength);

			std::string str(buf.begin(), buf.end());
			msg = json::parse(str);

			enet_packet_destroy(netEvent.packet);
			return 0;
		}
		case ENET_EVENT_TYPE_DISCONNECT:
			// Return -2 code to indicate we have lost connection to the server
			return -2;
		}
	}

	return -1;
}

void SlippiMatchmaking::MatchmakeThread()
{
	while (IsSearching())
	{
		if (isMmTerminated)
		{
			break;
		}

		switch (m_state)
		{
		case ProcessState::INITIALIZING:
			startMatchmaking();
			break;
		case ProcessState::MATCHMAKING:
			handleMatchmaking();
			break;
		case ProcessState::OPPONENT_CONNECTING:
			handleConnecting();
			break;
		}
	}

	// Clean up ENET connections
	terminateMmConnection();
}

void SlippiMatchmaking::disconnectFromServer()
{
	isMmConnected = false;

	if (m_server)
		enet_peer_disconnect(m_server, 0);
	else
		return;

	ENetEvent netEvent;
	while (enet_host_service(m_client, &netEvent, 3000) > 0)
	{
		switch (netEvent.type)
		{
		case ENET_EVENT_TYPE_RECEIVE:
			enet_packet_destroy(netEvent.packet);
			break;
		case ENET_EVENT_TYPE_DISCONNECT:
			m_server = nullptr;
			return;
		default:
			break;
		}
	}

	// didn't disconnect gracefully force disconnect
	enet_peer_reset(m_server);
	m_server = nullptr;
}

void SlippiMatchmaking::terminateMmConnection()
{
	// Disconnect from server
	disconnectFromServer();

	// Destroy client
	if (m_client)
	{
		enet_host_destroy(m_client);
		m_client = nullptr;
	}
}

// Fallback: arbitrarily choose the last available local IP address listed. They seem to be listed in decreasing order
// IE. 192.168.0.100 > 192.168.0.10 > 10.0.0.2
static char *getLocalAddressFallback()
{
	char host[256];
	int hostname = gethostname(host, sizeof(host)); // find the host name
	if (hostname == -1)
	{
		ERROR_LOG(SLIPPI_ONLINE, "[Matchmaking] Error finding LAN address");
		return nullptr;
	}

	struct hostent *host_entry = gethostbyname(host); // find host information
	if (host_entry == NULL || host_entry->h_addrtype != AF_INET || host_entry->h_addr_list[0] == 0)
	{
		ERROR_LOG(SLIPPI_ONLINE, "[Matchmaking] Error finding LAN host");
		return nullptr;
	}

	// Fetch the last IP (because that was correct for me, not sure if it will be for all)
	for (int i = 0; host_entry->h_addr_list[i] != 0; i++)
		if (host_entry->h_addr_list[i + 1] == 0)
			return host_entry->h_addr_list[i];
	return nullptr;
}

// Set up and connect a socket (UDP, so "connect" doesn't actually send any
// packets) so that the OS will determine what device/local IP address we will
// actually use.
static enet_uint32 getLocalAddress(ENetAddress *mm_address)
{
	ENetSocket socket = enet_socket_create(ENET_SOCKET_TYPE_DATAGRAM);
	if (socket == -1)
	{
		ERROR_LOG(SLIPPI_ONLINE, "[Matchmaking] Failed to get local address: socket create");
		enet_socket_destroy(socket);
		return 0;
	}

	if (enet_socket_connect(socket, mm_address) == -1)
	{
		ERROR_LOG(SLIPPI_ONLINE, "[Matchmaking] Failed to get local address: socket connect");
		enet_socket_destroy(socket);
		return 0;
	}

	ENetAddress enetAddress;
	if (enet_socket_get_address(socket, &enetAddress) == -1)
	{
		ERROR_LOG(SLIPPI_ONLINE, "[Matchmaking] Failed to get local address: socket get address");
		enet_socket_destroy(socket);
		return 0;
	}

	enet_socket_destroy(socket);
	return enetAddress.host;
}

void SlippiMatchmaking::startMatchmaking()
{
	// I don't understand why I have to do this... if I don't do this, rand always returns the
	// same value
	m_client = nullptr;

	int retryCount = 0;

	// If IsFixedRules mode, don't allow ISO's that are known to desync
	if (IsFixedRulesMode(m_searchSettings.mode))
	{
		auto check = slprs_get_iso_md5_check(slprs_exi_device_ptr);
		while (check.result == 0)
		{
			Common::SleepCurrentThread(500);
			retryCount++;
			if (retryCount > 10)
			{
				m_state = ProcessState::ERROR_ENCOUNTERED;
				m_errorMsg = "Could not validate ISO";
				return;
			}
			check = slprs_get_iso_md5_check(slprs_exi_device_ptr);
		}

		if (check.result == 2)
		{
			m_state = ProcessState::ERROR_ENCOUNTERED;
			m_errorMsg = "Cannot queue for this mode with a modded ISO known to desync";
			return;
		}
	}

	retryCount = 0;
	auto userInfo = m_user->GetUserInfo();
	while (m_client == nullptr && retryCount < 15)
	{
		bool customPort = SConfig::GetInstance().m_slippiForceNetplayPort;

		if (customPort)
			m_hostPort = SConfig::GetInstance().m_slippiNetplayPort;
		else
			m_hostPort = 41000 + (generator() % 10000);
		ERROR_LOG(SLIPPI_ONLINE, "[Matchmaking] Port to use: %d...", m_hostPort);

		// We are explicitly setting the client address because we are trying to utilize our connection
		// to the matchmaking service in order to hole punch. This port will end up being the port
		// we listen on when we start our server
		ENetAddress clientAddr;
		clientAddr.host = ENET_HOST_ANY;
		clientAddr.port = m_hostPort;

		m_client = enet_host_create(&clientAddr, 1, 3, 0, 0);
		retryCount++;
	}

	if (m_client == nullptr)
	{
		// Failed to create client
		m_state = ProcessState::ERROR_ENCOUNTERED;
		m_errorMsg = "Failed to create mm client";
		ERROR_LOG(SLIPPI_ONLINE, "[Matchmaking] Failed to create client...");
		return;
	}

	ENetAddress addr;
	enet_address_set_host(&addr, MM_HOST.c_str());
	addr.port = MM_PORT;

	m_server = enet_host_connect(m_client, &addr, 3, 0);

	if (m_server == nullptr)
	{
		// Failed to connect to server
		m_state = ProcessState::ERROR_ENCOUNTERED;
		m_errorMsg = "Failed to start connection to mm server";
		ERROR_LOG(SLIPPI_ONLINE, "[Matchmaking] Failed to start connection to mm server...");
		return;
	}

	// Before we can request a ticket, we must wait for connection to be successful
	int connectAttemptCount = 0;
	while (!isMmConnected)
	{
		ENetEvent netEvent;
		int net = enet_host_service(m_client, &netEvent, 500);
		if (net <= 0 || netEvent.type != ENET_EVENT_TYPE_CONNECT)
		{
			// Not yet connected, will retry
			connectAttemptCount++;
			if (connectAttemptCount >= 20)
			{
				ERROR_LOG(SLIPPI_ONLINE, "[Matchmaking] Failed to connect to mm server...");
				m_state = ProcessState::ERROR_ENCOUNTERED;
				m_errorMsg = "Failed to connect to mm server";
				return;
			}

			continue;
		}

		netEvent.peer->data = &userInfo.displayName;
		m_client->intercept = ENetUtil::InterceptCallback;
		isMmConnected = true;
		ERROR_LOG(SLIPPI_ONLINE, "[Matchmaking] Connected to mm server...");
	}

	ERROR_LOG(SLIPPI_ONLINE, "[Matchmaking] Trying to find match...");

	/*if (!m_user->IsLoggedIn())
	{
	    ERROR_LOG(SLIPPI_ONLINE, "[Matchmaking] Must be logged in to queue");
	    m_state = ProcessState::ERROR_ENCOUNTERED;
	    m_errorMsg = "Must be logged in to queue. Go back to menu";
	    return;
	}*/

	// Determine local IP address. We can attempt to connect to our opponent via
	// local IP address if we have the same external IP address. The following
	// scenarios can cause us to have the same external IP address:
	// - we are connected to the same LAN
	// - we are connected to the same VPN node
	// - we are behind the same CGNAT
	char lanAddr[30] = "";
	if (SConfig::GetInstance().m_slippiForceLanIp)
	{
		WARN_LOG(SLIPPI_ONLINE, "[Matchmaking] Overwriting LAN IP sent with configured address");
		sprintf(lanAddr, "%s:%d", SConfig::GetInstance().m_slippiLanIp.c_str(), m_hostPort);
	}
	else
	{
		enet_uint32 localAddress = getLocalAddress(&addr);
		if (localAddress != 0)
		{
			sprintf(lanAddr, "%s:%d", inet_ntoa(*(struct in_addr *)&localAddress), m_hostPort);
		}
		else
		{
			char *fallbackAddress = getLocalAddressFallback();
			if (fallbackAddress != nullptr)
				sprintf(lanAddr, "%s:%d", inet_ntoa(*(struct in_addr *)fallbackAddress), m_hostPort);
		}
	}
	WARN_LOG(SLIPPI_ONLINE, "[Matchmaking] Sending LAN address: %s", lanAddr);

	std::vector<u8> connectCodeBuf;
	connectCodeBuf.insert(connectCodeBuf.end(), m_searchSettings.connectCode.begin(),
	                      m_searchSettings.connectCode.end());

	// Send message to server to create ticket
	json request;
	request["type"] = MmMessageType::CREATE_TICKET;
	request["user"] = {{"uid", userInfo.uid},
	                   {"playKey", userInfo.playKey},
	                   {"connectCode", userInfo.connectCode},
	                   {"displayName", userInfo.displayName}};
	request["search"] = {{"mode", m_searchSettings.mode}, {"connectCode", connectCodeBuf}};
	request["appVersion"] = scm_slippi_semver_str;
	request["ipAddressLan"] = lanAddr;

	// Couch co-op: declare a second local player on this ticket. When a user2.json file
	// exists next to user.json, the second seat queues as that account; otherwise the
	// second player joins as a guest (only accepted by the server in freeplay modes)
	if (SConfig::GetInstance().bSlippiCouchCoopPort2 >= 0)
	{
		request["localPlayerCount"] = 2;

		bool hasUser2 = false;
		std::string user2Path = File::GetSlippiUserConfigFolder() + DIR_SEP + "user2.json";
		std::string user2Contents;
		if (File::Exists(user2Path) && File::ReadFileToString(user2Path, user2Contents))
		{
			json user2 = json::parse(user2Contents, nullptr, false);
			if (!user2.is_discarded() && user2.value("uid", "") != "" && user2.value("playKey", "") != "")
			{
				json user2Player = {{"uid", user2.value("uid", "")},
				                    {"playKey", user2.value("playKey", "")},
				                    {"connectCode", user2.value("connectCode", "")},
				                    {"displayName", user2.value("displayName", "")}};
				request["localPlayers"] = json::array({user2Player});
				hasUser2 = true;
			}
			else
			{
				ERROR_LOG(SLIPPI_ONLINE, "[Matchmaking] user2.json exists but is invalid, queueing P2 as guest");
			}
		}

		if (!hasUser2)
		{
			request["guestName"] = SConfig::GetInstance().m_slippiCouchCoopGuestName;
		}
	}

	sendMessage(request);

	// Get response from server
	json response;
	int rcvRes = receiveMessage(response, 5000);
	if (rcvRes != 0)
	{
		ERROR_LOG(SLIPPI_ONLINE, "[Matchmaking] Did not receive response from server for create ticket");
		m_state = ProcessState::ERROR_ENCOUNTERED;
		m_errorMsg = "Failed to join mm queue";
		return;
	}

	std::string respType = response["type"];
	if (respType != MmMessageType::CREATE_TICKET_RESP)
	{
		ERROR_LOG(SLIPPI_ONLINE, "[Matchmaking] Received incorrect response for create ticket");
		ERROR_LOG(SLIPPI_ONLINE, "%s", response.dump().c_str());
		m_state = ProcessState::ERROR_ENCOUNTERED;
		m_errorMsg = "Invalid response when joining mm queue";
		return;
	}

	std::string err = response.value("error", "");
	if (err.length() > 0)
	{
		ERROR_LOG(SLIPPI_ONLINE, "[Matchmaking] Received error from server for create ticket");
		m_state = ProcessState::ERROR_ENCOUNTERED;
		m_errorMsg = err;
		return;
	}

	m_state = ProcessState::MATCHMAKING;
	ERROR_LOG(SLIPPI_ONLINE, "[Matchmaking] Request ticket success");
}

void SlippiMatchmaking::handleMatchmaking()
{
	// Deal with class shut down
	if (m_state != ProcessState::MATCHMAKING)
		return;

	// Get response from server
	json getResp;
	int rcvRes = receiveMessage(getResp, 2000);
	if (rcvRes == -1)
	{
		INFO_LOG(SLIPPI_ONLINE, "[Matchmaking] Have not yet received assignment");
		return;
	}
	else if (rcvRes != 0)
	{
		// Right now the only other code is -2 meaning the server died probably?
		ERROR_LOG(SLIPPI_ONLINE, "[Matchmaking] Lost connection to the mm server");
		m_state = ProcessState::ERROR_ENCOUNTERED;
		m_errorMsg = "Lost connection to the mm server";
		return;
	}

	std::string respType = getResp["type"];
	if (respType != MmMessageType::GET_TICKET_RESP)
	{
		ERROR_LOG(SLIPPI_ONLINE, "[Matchmaking] Received incorrect response for get ticket");
		m_state = ProcessState::ERROR_ENCOUNTERED;
		m_errorMsg = "Invalid response when getting mm status";
		return;
	}

	std::string err = getResp.value("error", "");
	std::string latestVersion = getResp.value("latestVersion", "");
	if (err.length() > 0)
	{
		if (latestVersion != "")
		{
			// Update version number when the mm server tells us our version is outdated
			m_user->OverwriteLatestVersion(
			    latestVersion); // Force latest version for people whose file updates dont work
		}

		ERROR_LOG(SLIPPI_ONLINE, "[Matchmaking] Received error from server for get ticket");
		m_state = ProcessState::ERROR_ENCOUNTERED;
		m_errorMsg = err;
		return;
	}

	m_isSwapAttempt = false;
	m_netplayClient = nullptr;

	// Clear old users
	m_remoteIps.clear();
	m_remotePlayerIdxsByConn.clear();
	m_playerInfo.clear();
	m_localPlayerIndices.clear();

	std::string matchId = getResp.value("matchId", "");
	WARN_LOG(SLIPPI_ONLINE, "Match ID: %s", matchId.c_str());

	auto queue = getResp["players"];
	if (queue.is_array())
	{
		std::string localExternalIp = "";

		for (json::iterator it = queue.begin(); it != queue.end(); ++it)
		{
			json el = *it;
			SlippiUser::UserInfo playerInfo;

			bool isLocal = el.value("isLocalPlayer", false);
			playerInfo.uid = el.value("uid", "");
			playerInfo.displayName = el.value("displayName", "");
			playerInfo.connectCode = el.value("connectCode", "");
			playerInfo.port = el.value("port", 0);
			playerInfo.isBot = el.value("isBot", false);

			if (el["chatMessages"].is_array())
			{
				playerInfo.chatMessages = el.value("chatMessages", m_user->GetDefaultChatMessages());

				if (playerInfo.chatMessages.size() != 16)
				{
					playerInfo.chatMessages = m_user->GetDefaultChatMessages();
				}
			}
			else
			{
				playerInfo.chatMessages = m_user->GetDefaultChatMessages();
			}

			json rankEl = el["rank"];
			if (rankEl.is_object())
			{
				playerInfo.rankedRating = rankEl.value("rating", 0.0f);
				playerInfo.rankedUpdateCount = rankEl.value("updateCount", 0);
				playerInfo.rankedGlobalPlacement = rankEl.value("globalPlacement", 0);
				playerInfo.rankedRegionalPlacement = rankEl.value("regionalPlacement", 0);
			}

			m_playerInfo.push_back(playerInfo);

			if (isLocal)
			{
				std::vector<std::string> localIpParts;
				SplitString(el.value("ipAddress", "1.1.1.1:123"), ':', localIpParts);
				localExternalIp = localIpParts[0];

				// Ports come straight from the server response; a missing/zero `port`
				// would yield index -1 → (u8)255 and index fixed-size arrays out of
				// bounds downstream (playerActive & co). Never accept it.
				int localPlayerIdx = playerInfo.port - 1;
				if (localPlayerIdx < 0 || localPlayerIdx >= SLIPPI_PLAYER_COUNT_MAX)
				{
					ERROR_LOG(SLIPPI_ONLINE, "[Matchmaking] Local player entry has invalid port: %d", playerInfo.port);
				}
				else
				{
					m_localPlayerIndices.push_back(localPlayerIdx);
				}
			}
		};

		// Keep m_localPlayerIndex pointing at the first local player for backward compat
		std::sort(m_localPlayerIndices.begin(), m_localPlayerIndices.end());
		// The netplay client clamps its local list to SLIPPI_LOCAL_PLAYER_MAX but
		// RemotePlayerCount() is derived from THIS list — if they disagree, every
		// port <-> remote-index mapping diverges and the match desyncs from frame 1.
		// A response with too many local seats is malformed: refuse it outright.
		if (m_localPlayerIndices.size() > SLIPPI_LOCAL_PLAYER_MAX)
		{
			ERROR_LOG(SLIPPI_ONLINE, "[Matchmaking] Server declared %d local players, max supported is %d",
			          (int)m_localPlayerIndices.size(), SLIPPI_LOCAL_PLAYER_MAX);
			m_state = ProcessState::ERROR_ENCOUNTERED;
			m_errorMsg = "Matchmaking returned an invalid match (too many local players)";
			return;
		}
		if (!m_localPlayerIndices.empty())
			m_localPlayerIndex = m_localPlayerIndices[0];

		// Loop a second time to get the correct remote IPs

		// One dedupe key per m_remoteIps entry, alive only while parsing this
		// response. A key exists only when the server explicitly provided both
		// addresses for the entry, so players that fell back to the placeholder
		// literals can never be silently merged onto one connection.
		std::vector<std::string> remoteConnDedupeKeys;

		for (json::iterator it = queue.begin(); it != queue.end(); ++it)
		{
			json el = *it;

			int elPlayerIdx = el.value("port", 0) - 1;
			if (std::find(m_localPlayerIndices.begin(), m_localPlayerIndices.end(), elPlayerIdx) !=
			    m_localPlayerIndices.end())
				continue;

			// Same rule as for local entries: an out-of-range index would corrupt the
			// fixed-size per-player arrays (playerActive[(u8)-1] writes past the array
			// on the netplay thread). Skip the entry — the resulting missing
			// connection surfaces as a clean connect error instead
			if (elPlayerIdx < 0 || elPlayerIdx >= SLIPPI_PLAYER_COUNT_MAX)
			{
				ERROR_LOG(SLIPPI_ONLINE, "[Matchmaking] Remote player entry has invalid port: %d",
				          el.value("port", 0));
				continue;
			}

			auto extIp = el.value("ipAddress", "1.1.1.1:123");
			std::vector<std::string> exIpParts;
			SplitString(extIp, ':', exIpParts);

			auto lanIp = el.value("ipAddressLan", "1.1.1.1:123");

			WARN_LOG(SLIPPI_ONLINE, "LAN IP: %s", lanIp.c_str());

			// TODO: Instead of using one or the other, it might be better to try both

			// If external IPs are different, just use that address.
			// If external IPs are the same, try using LAN IPs
			std::string connectIp = (exIpParts[0] != localExternalIp || lanIp.empty()) ? extIp : lanIp;

			// Dedupe couch peers: a couch peer lists one entry per player it hosts but
			// only has one client to connect to; track every player index served by
			// each connection. Merging is deliberately strict — it requires BOTH the
			// external (ip:port) and LAN strings to be explicitly provided and
			// identical, and at most SLIPPI_LOCAL_PLAYER_MAX players per connection.
			// An accidental merge of two distinct peers (shared CGNAT, missing server
			// fields) would under-provision connections and stall the match with no
			// error, so when in doubt we keep one connection per player and let it
			// fail loudly at connect time instead.
			bool hasExplicitAddrs = el.value("ipAddress", "") != "" && el.value("ipAddressLan", "") != "";
			std::string dedupeKey = extIp + "|" + lanIp;
			bool isKnownConn = false;
			if (hasExplicitAddrs)
			{
				for (int i = 0; i < (int)remoteConnDedupeKeys.size(); i++)
				{
					if (remoteConnDedupeKeys[i] != dedupeKey)
						continue;
					if (m_remotePlayerIdxsByConn[i].size() >= SLIPPI_LOCAL_PLAYER_MAX)
					{
						ERROR_LOG(SLIPPI_ONLINE,
						          "[Matchmaking] More than %d players share address %s — not merging further",
						          SLIPPI_LOCAL_PLAYER_MAX, connectIp.c_str());
						break;
					}
					m_remotePlayerIdxsByConn[i].push_back((u8)elPlayerIdx);
					isKnownConn = true;
					break;
				}
			}
			if (!isKnownConn)
			{
				m_remoteIps.push_back(connectIp);
				m_remotePlayerIdxsByConn.push_back(std::vector<u8>{(u8)elPlayerIdx});
				remoteConnDedupeKeys.push_back(dedupeKey);
			}
		}
	}
	m_isHost = getResp.value("isHost", false);

	// Get allowed stages. For stage select modes like direct and teams, this will only impact the first map selected
	m_allowedStages.clear();
	auto stages = getResp["stages"];
	if (stages.is_array())
	{
		for (json::iterator it = stages.begin(); it != stages.end(); ++it)
		{
			json el = *it;
			auto stageId = el.get<int>();
			m_allowedStages.push_back(stageId);
		}
	}

	if (m_allowedStages.empty())
	{
		// Default case, shouldn't ever really be hit but it's here just in case
		m_allowedStages.push_back(0x3);  // Pokemon
		m_allowedStages.push_back(0x8);  // Yoshi's Story
		m_allowedStages.push_back(0x1C); // Dream Land
		m_allowedStages.push_back(0x1F); // Battlefield
		m_allowedStages.push_back(0x20); // Final Destination

		// Add FoD if singles
		if (m_playerInfo.size() == 2)
		{
			m_allowedStages.push_back(0x2); // FoD
		}
	}

	m_mmResult.id = matchId;
	m_mmResult.players = m_playerInfo;
	m_mmResult.stages = m_allowedStages;
	m_mmResult.items = getResp.value<u32>("items", 0);

	// Disconnect and destroy enet client to mm server
	terminateMmConnection();

	// If ranked, report to backend that we are attempting to connect to this match
	if (matchId.find("mode.ranked") != std::string::npos)
	{
		slprs_exi_device_report_match_status(slprs_exi_device_ptr, matchId.c_str(), "connecting", true);
	}

	m_state = ProcessState::OPPONENT_CONNECTING;
	ERROR_LOG(SLIPPI_ONLINE, "[Matchmaking] Opponent found. isDecider: %s", m_isHost ? "true" : "false");
}

int SlippiMatchmaking::LocalPlayerIndex()
{
	return m_localPlayerIndex;
}

std::vector<int> SlippiMatchmaking::GetLocalPlayerIndices()
{
	return m_localPlayerIndices;
}

std::vector<SlippiUser::UserInfo> SlippiMatchmaking::GetPlayerInfo()
{
	return m_playerInfo;
}

std::vector<u16> SlippiMatchmaking::GetStages()
{
	return m_allowedStages;
}

// This is kind of duplicate code from what exists in rust. Maybe eventually it should be remove
// and exist only on the rust side
SlippiMatchmaking::SlippiRank SlippiMatchmaking::GetPlayerRank(u8 port)
{
	if (port >= m_playerInfo.size())
	{
		return SlippiRank::Unranked;
	}

	auto info = m_playerInfo[port];

	float rating = info.rankedRating;
	int updateCount = info.rankedUpdateCount;
	int global = info.rankedGlobalPlacement;
	int regional = info.rankedRegionalPlacement;

	if (updateCount < 5)
	{
		return SlippiRank::Unranked;
	}

	if (rating <= 765.42f)
	{
		return SlippiRank::Bronze1;
	}

	if (rating > 765.43f && rating <= 913.71f)
	{
		return SlippiRank::Bronze2;
	}

	if (rating > 913.72f && rating <= 1054.86f)
	{
		return SlippiRank::Bronze3;
	}

	if (rating > 1054.87f && rating <= 1188.87f)
	{
		return SlippiRank::Silver1;
	}

	if (rating > 1188.88f && rating <= 1315.74f)
	{
		return SlippiRank::Silver2;
	}

	if (rating > 1315.75f && rating <= 1435.47f)
	{
		return SlippiRank::Silver3;
	}

	if (rating > 1435.48f && rating <= 1548.06f)
	{
		return SlippiRank::Gold1;
	}

	if (rating > 1548.07f && rating <= 1653.51f)
	{
		return SlippiRank::Gold2;
	}

	if (rating > 1653.52f && rating <= 1751.82f)
	{
		return SlippiRank::Gold3;
	}

	if (rating > 1751.83f && rating <= 1842.99f)
	{
		return SlippiRank::Platinum1;
	}

	if (rating > 1843.0f && rating <= 1927.02f)
	{
		return SlippiRank::Platinum2;
	}

	if (rating > 1927.03f && rating <= 2003.91f)
	{
		return SlippiRank::Platinum3;
	}

	if (rating > 2003.92f && rating <= 2073.66f)
	{
		return SlippiRank::Diamond1;
	}

	if (rating > 2073.67f && rating <= 2136.27f)
	{
		return SlippiRank::Diamond2;
	}

	if (rating > 2136.28f && rating <= 2191.74f)
	{
		return SlippiRank::Diamond3;
	}

	if (rating >= 2191.75f && (global > 0 || regional > 0))
	{
		return SlippiRank::Grandmaster;
	}

	if (rating > 2191.75f && rating <= 2274.99f)
	{
		return SlippiRank::Master1;
	}

	if (rating > 2275.0f && rating <= 2350.0f)
	{
		return SlippiRank::Master2;
	}

	if (rating > 2350.0f)
	{
		return SlippiRank::Master3;
	}

	return SlippiRank::Unranked;
}

SlippiMatchmaking::MatchmakeResult SlippiMatchmaking::GetMatchmakeResult()
{
	return m_mmResult;
}

std::string SlippiMatchmaking::GetPlayerName(u8 port)
{
	if (port >= m_playerInfo.size())
	{
		return "";
	}
	return m_playerInfo[port].displayName;
}

u8 SlippiMatchmaking::RemotePlayerCount()
{
	if (m_playerInfo.size() == 0)
		return 0;

	// Assume a single local player if the server response didn't flag any (legacy behavior)
	size_t localPlayerCount = m_localPlayerIndices.empty() ? 1 : m_localPlayerIndices.size();
	// Saturate: if local entries somehow outnumber players (malformed/duplicated
	// response), the subtraction would underflow size_t and the u8 cast would
	// produce a huge count that indexes fixed-size arrays out of bounds downstream
	if (localPlayerCount >= m_playerInfo.size())
		return 0;
	return (u8)(m_playerInfo.size() - localPlayerCount);
}

void SlippiMatchmaking::handleConnecting()
{
	auto userInfo = m_user->GetUserInfo();

	m_isSwapAttempt = false;
	m_netplayClient = nullptr;

	u8 remotePlayerCount = RemotePlayerCount();
	std::vector<std::string> remoteParts;
	std::vector<std::string> addrs;
	std::vector<u16> ports;
	for (int i = 0; i < m_remoteIps.size(); i++)
	{
		remoteParts.clear();
		SplitString(m_remoteIps[i], ':', remoteParts);
		addrs.push_back(remoteParts[0]);
		ports.push_back(std::stoi(remoteParts[1]));
	}

	std::vector<u8> localPlayerIdxs;
	for (int i = 0; i < m_localPlayerIndices.size(); i++)
	{
		localPlayerIdxs.push_back((u8)m_localPlayerIndices[i]);
	}
	if (localPlayerIdxs.empty())
		localPlayerIdxs.push_back((u8)m_localPlayerIndex);

	std::stringstream ipLog;
	ipLog << "Remote player IPs: ";
	for (int i = 0; i < m_remoteIps.size(); i++)
	{
		ipLog << m_remoteIps[i] << ", ";
	}
	// INFO_LOG(SLIPPI_ONLINE, "[Matchmaking] My port: %d || %s", m_hostPort, ipLog.str());

	// Is host is now used to specify who the decider is
	auto client = std::make_unique<SlippiNetplayClient>(addrs, ports, m_remotePlayerIdxsByConn, remotePlayerCount,
	                                                    m_hostPort, m_isHost, localPlayerIdxs);

	while (!m_netplayClient)
	{
		auto status = client->GetSlippiConnectStatus();
		if (status == SlippiNetplayClient::SlippiConnectStatus::NET_CONNECT_STATUS_INITIATED)
		{
			INFO_LOG(SLIPPI_ONLINE, "[Matchmaking] Connection not yet successful");
			Common::SleepCurrentThread(500);

			// Deal with class shut down
			if (m_state != ProcessState::OPPONENT_CONNECTING)
				return;

			continue;
		}
		else if (status == SlippiNetplayClient::SlippiConnectStatus::NET_CONNECT_STATUS_FAILED &&
		         m_searchSettings.mode == SlippiMatchmaking::OnlinePlayMode::TEAMS)
		{
			// If we failed setting up a connection in teams mode, show a detailed error about who we had issues
			// connecting to.
			ERROR_LOG(SLIPPI_ONLINE, "[Matchmaking] Failed to connect to players");
			m_state = ProcessState::ERROR_ENCOUNTERED;
			m_errorMsg = "Timed out waiting for other players to connect";
			auto failedConns = client->GetFailedConnections();
			if (!failedConns.empty())
			{
				std::stringstream err;
				err << "Could not connect to players: ";
				bool isFirstName = true;
				for (int i = 0; i < failedConns.size(); i++)
				{
					int connIdx = failedConns[i];
					if (connIdx >= m_remotePlayerIdxsByConn.size())
						continue;

					// A failed couch connection means every player it serves is unreachable
					for (auto p : m_remotePlayerIdxsByConn[connIdx])
					{
						if (p >= m_playerInfo.size())
							continue;

						if (!isFirstName)
						{
							err << ", ";
						}
						err << m_playerInfo[p].displayName;
						isFirstName = false;
					}
				}
				m_errorMsg = err.str();
			}

			return;
		}
		else if (status != SlippiNetplayClient::SlippiConnectStatus::NET_CONNECT_STATUS_CONNECTED)
		{
			ERROR_LOG(SLIPPI_ONLINE, "[Matchmaking] Connection attempt failed, looking for someone else.");

			// Return to the start to get a new ticket to find someone else we can hopefully connect with
			m_netplayClient = nullptr;
			m_state = ProcessState::INITIALIZING;
			return;
		}

		ERROR_LOG(SLIPPI_ONLINE, "[Matchmaking] Connection success!");

		// Successful connection
		m_netplayClient = std::move(client);
	}

	// Connection success, our work is done
	m_state = ProcessState::CONNECTION_SUCCESS;
}
