package com.darkwisp.app.viewmodel

import android.app.Application
import android.util.Log
import androidx.lifecycle.AndroidViewModel
import androidx.lifecycle.viewModelScope
import com.darkwisp.app.nostr.ClientMessage
import com.darkwisp.app.nostr.Filter
import com.darkwisp.app.nostr.Nip02
import com.darkwisp.app.nostr.Nip10
import com.darkwisp.app.nostr.Nip51
import com.darkwisp.app.nostr.Nip65
import com.darkwisp.app.nostr.Nip85
import com.darkwisp.app.nostr.NipA3
import com.darkwisp.app.nostr.SimpleGroupEntry
import com.darkwisp.app.nostr.LocalSigner
import com.darkwisp.app.nostr.NostrEvent
import com.darkwisp.app.nostr.NostrSigner
import com.darkwisp.app.nostr.ProfileData
import com.darkwisp.app.relay.OutboxRouter
import com.darkwisp.app.relay.RelayConfig
import com.darkwisp.app.relay.RelayPool
import com.darkwisp.app.repo.ContactRepository
import com.darkwisp.app.repo.EventRepository
import com.darkwisp.app.repo.DiscoveryState
import com.darkwisp.app.repo.ExtendedNetworkRepository
import com.darkwisp.app.repo.FollowerRepository
import com.darkwisp.app.repo.KeyRepository
import com.darkwisp.app.repo.PaymentTargetRepository
import com.darkwisp.app.repo.RelayHintStore
import com.darkwisp.app.repo.RelayListRepository
import com.darkwisp.app.relay.SubscriptionManager
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeoutOrNull

enum class ProfileSortMode(val labelResId: Int) {
    RECENCY(com.darkwisp.app.R.string.profile_sort_recent),
    LIKES(com.darkwisp.app.R.string.profile_sort_likes),
    REPOSTS(com.darkwisp.app.R.string.profile_sort_reposts),
    ZAPS(com.darkwisp.app.R.string.profile_sort_zaps),
    REPLIES(com.darkwisp.app.R.string.profile_sort_replies)
}

private fun ProfileSortMode.relaySlug() = when (this) {
    ProfileSortMode.LIKES -> "likes"
    ProfileSortMode.REPOSTS -> "reposts"
    ProfileSortMode.ZAPS -> "zaps"
    ProfileSortMode.REPLIES -> "replies"
    ProfileSortMode.RECENCY -> error("No relay URL for recency")
}

class UserProfileViewModel(app: Application) : AndroidViewModel(app) {
    private val keyRepo = KeyRepository(app)

    private val _profile = MutableStateFlow<ProfileData?>(null)
    val profile: StateFlow<ProfileData?> = _profile

    private val _isFollowing = MutableStateFlow(false)
    val isFollowing: StateFlow<Boolean> = _isFollowing

    private val _rootNotes = MutableStateFlow<List<NostrEvent>>(emptyList())
    val rootNotes: StateFlow<List<NostrEvent>> = _rootNotes

    private val _replies = MutableStateFlow<List<NostrEvent>>(emptyList())
    val replies: StateFlow<List<NostrEvent>> = _replies

    // Track which root notes are reposts: inner event id -> reposter pubkey
    private val _repostAuthors = mutableMapOf<String, String>()
    val repostAuthors: Map<String, String> get() = _repostAuthors

    // Track repost sort time: inner event id -> repost wrapper created_at
    private val _repostSortTime = mutableMapOf<String, Long>()

    private val _followList = MutableStateFlow<List<Nip02.FollowEntry>>(emptyList())
    val followList: StateFlow<List<Nip02.FollowEntry>> = _followList

    private val _relayList = MutableStateFlow<List<RelayConfig>>(emptyList())
    val relayList: StateFlow<List<RelayConfig>> = _relayList

    private val _paymentTargets = MutableStateFlow<List<NipA3.PaymentTarget>>(emptyList())
    val paymentTargets: StateFlow<List<NipA3.PaymentTarget>> = _paymentTargets

    private val _pinnedNoteIds = MutableStateFlow<Set<String>>(emptySet())
    val pinnedNoteIds: StateFlow<Set<String>> = _pinnedNoteIds

    private val _relayHints = MutableStateFlow<Set<String>>(emptySet())
    val relayHints: StateFlow<Set<String>> = _relayHints

    private val _followedBy = MutableStateFlow<List<String>>(emptyList())
    val followedBy: StateFlow<List<String>> = _followedBy

    private val _followProfileVersion = MutableStateFlow(0)
    val followProfileVersion: StateFlow<Int> = _followProfileVersion

    private val _notesSortMode = MutableStateFlow(ProfileSortMode.RECENCY)
    val notesSortMode: StateFlow<ProfileSortMode> = _notesSortMode

    private val _repliesSortMode = MutableStateFlow(ProfileSortMode.RECENCY)
    val repliesSortMode: StateFlow<ProfileSortMode> = _repliesSortMode

    private val _sortedNotes = MutableStateFlow<List<NostrEvent>>(emptyList())
    val sortedNotes: StateFlow<List<NostrEvent>> = _sortedNotes

    private val _sortedNotesLoading = MutableStateFlow(false)
    val sortedNotesLoading: StateFlow<Boolean> = _sortedNotesLoading

    private val _sortedReplies = MutableStateFlow<List<NostrEvent>>(emptyList())
    val sortedReplies: StateFlow<List<NostrEvent>> = _sortedReplies

    private val _sortedRepliesLoading = MutableStateFlow(false)
    val sortedRepliesLoading: StateFlow<Boolean> = _sortedRepliesLoading

    private val _followers = MutableStateFlow<List<ProfileData>>(emptyList())
    val followers: StateFlow<List<ProfileData>> = _followers

    private val _followersLoading = MutableStateFlow(false)
    val followersLoading: StateFlow<Boolean> = _followersLoading

    private val _followersError = MutableStateFlow(false)
    val followersError: StateFlow<Boolean> = _followersError

    private val _followerCount = MutableStateFlow<Int?>(null)
    val followerCount: StateFlow<Int?> = _followerCount

    private val _followerCountSource = MutableStateFlow<String?>(null)
    val followerCountSource: StateFlow<String?> = _followerCountSource

    private val _galleryPosts = MutableStateFlow<List<NostrEvent>>(emptyList())
    val galleryPosts: StateFlow<List<NostrEvent>> = _galleryPosts

    private val _groups = MutableStateFlow<List<SimpleGroupEntry>>(emptyList())
    val groups: StateFlow<List<SimpleGroupEntry>> = _groups

    private val _groupsLoading = MutableStateFlow(false)
    val groupsLoading: StateFlow<Boolean> = _groupsLoading

    private var targetPubkey: String = ""
    private var eventRepoRef: EventRepository? = null
    private var relayPoolRef: RelayPool? = null
    private var outboxRouterRef: OutboxRouter? = null
    private var subManagerRef: SubscriptionManager? = null
    private var relayHintStoreRef: RelayHintStore? = null
    private var relayListRepoRef: RelayListRepository? = null
    private var paymentTargetRepoRef: PaymentTargetRepository? = null
    private var followerRepoRef: FollowerRepository? = null
    private val activeEngagementSubIds = mutableListOf<String>()
    private val activeFollowProfileSubIds = mutableListOf<String>()
    private var topRelayUrls: List<String> = emptyList()

    private var extendedNetworkRepoRef: ExtendedNetworkRepository? = null
    private var isLoadingMoreNotes = false
    private var isLoadingMoreReplies = false
    private var profileFeedNotesGen = 0
    private var profileFeedRepliesGen = 0
    private var profileFollowersGen = 0
    private var profileFeedNotesJob: Job? = null
    private var profileFeedRepliesJob: Job? = null
    private var profileFollowersJob: Job? = null
    private var followerCountJob: Job? = null
    private var followerCountGen = 0
    // Track oldest event timestamps from the target user (kind 1/6) for pagination.
    // rootNotes contains repost inner events with different authors/timestamps,
    // so we track the user's own event timestamps separately.
    private var oldestNoteTimestamp: Long = Long.MAX_VALUE
    private var oldestReplyTimestamp: Long = Long.MAX_VALUE
    private var latestProfileTimestamp: Long = 0
    private var latestFollowListTimestamp: Long = 0
    private var latestRelayListTimestamp: Long = 0

    companion object {
        private val SUB_IDS = setOf("userprofile", "userposts", "usergallery", "userfollows", "userrelays", "userpins", "usergroups", "userpaytargets", "followprofiles")
    }

    fun loadProfile(
        pubkey: String,
        eventRepo: EventRepository,
        contactRepo: ContactRepository,
        relayPool: RelayPool,
        outboxRouter: OutboxRouter? = null,
        relayListRepo: RelayListRepository? = null,
        subManager: SubscriptionManager? = null,
        topRelayUrls: List<String> = emptyList(),
        relayHintStore: RelayHintStore? = null,
        extendedNetworkRepo: ExtendedNetworkRepository? = null,
        paymentTargetRepo: PaymentTargetRepository? = null,
        followerRepo: FollowerRepository? = null
    ) {
        targetPubkey = pubkey
        eventRepoRef = eventRepo
        relayPoolRef = relayPool
        outboxRouterRef = outboxRouter
        subManagerRef = subManager
        relayHintStoreRef = relayHintStore
        relayListRepoRef = relayListRepo
        followerRepoRef = followerRepo
        this.topRelayUrls = topRelayUrls
        oldestNoteTimestamp = Long.MAX_VALUE
        oldestReplyTimestamp = Long.MAX_VALUE
        latestProfileTimestamp = 0
        latestFollowListTimestamp = 0
        latestRelayListTimestamp = 0
        _notesSortMode.value = ProfileSortMode.RECENCY
        _repliesSortMode.value = ProfileSortMode.RECENCY
        _sortedNotes.value = emptyList()
        _sortedNotesLoading.value = false
        _sortedReplies.value = emptyList()
        _sortedRepliesLoading.value = false
        _followers.value = emptyList()
        _followersLoading.value = false
        _followersError.value = false
        _followerCount.value = null
        _followerCountSource.value = null
        _galleryPosts.value = emptyList()
        _groups.value = emptyList()
        _groupsLoading.value = false
        _profile.value = eventRepo.getProfileData(pubkey)
        paymentTargetRepoRef = paymentTargetRepo
        _paymentTargets.value = paymentTargetRepo?.getTargets(pubkey) ?: emptyList()
        _relayHints.value = relayHintStore?.getHints(pubkey) ?: emptySet()
        _isFollowing.value = contactRepo.isFollowing(pubkey)
        extendedNetworkRepoRef = extendedNetworkRepo
        _followedBy.value = extendedNetworkRepo?.getFollowedBy(pubkey)?.toList() ?: emptyList()
        Log.d("UserProfileVM", "loadProfile: followedBy=${_followedBy.value.size} for $pubkey, discoveryState=${extendedNetworkRepo?.discoveryState?.value}")

        // If social graph is still being computed, re-query when discovery completes
        if (_followedBy.value.isEmpty() && extendedNetworkRepo != null) {
            viewModelScope.launch {
                extendedNetworkRepo.discoveryState.first { it is DiscoveryState.Complete || it is DiscoveryState.Failed }
                val result = extendedNetworkRepo.getFollowedBy(pubkey).toList()
                Log.d("UserProfileVM", "Discovery finished, followedBy=${result.size} for $pubkey")
                _followedBy.value = result
            }
        }

        // Close any prior subs (e.g. re-subscribe after relay list discovery)
        closeAllSubs(relayPool)

        // Request relay list for this user
        outboxRouter?.requestMissingRelayLists(listOf(pubkey))

        // Request fresh profile, posts, follow list, and relay list
        val profileFilter = Filter(kinds = listOf(0), authors = listOf(pubkey), limit = 1)
        val postsFilter = Filter(kinds = listOf(1, 6, 1068, 6969, 30023, 20, 21, 22), authors = listOf(pubkey), limit = 50)
        // Gallery posts can be old and curated — fetch separately with no since filter, higher limit
        val galleryFilter = Filter(kinds = listOf(20, 21, 22), authors = listOf(pubkey), limit = 100)
        val followFilter = Filter(kinds = listOf(3), authors = listOf(pubkey), limit = 1)
        val relayFilter = Filter(kinds = listOf(10002), authors = listOf(pubkey), limit = 1)
        val pinFilter = Filter(kinds = listOf(10001), authors = listOf(pubkey), limit = 1)
        val groupsFilter = Filter(kinds = listOf(Nip51.KIND_SIMPLE_GROUPS), authors = listOf(pubkey), limit = 1)
        val payTargetsFilter = Filter(kinds = listOf(NipA3.KIND), authors = listOf(pubkey), limit = 1)

        if (outboxRouter != null) {
            outboxRouter.subscribeToUserWriteRelays("userprofile", pubkey, profileFilter)
            outboxRouter.subscribeToUserWriteRelays("userposts", pubkey, postsFilter)
            outboxRouter.subscribeToUserWriteRelays("usergallery", pubkey, galleryFilter)
            outboxRouter.subscribeToUserWriteRelays("userfollows", pubkey, followFilter)
            outboxRouter.subscribeToUserWriteRelays("userrelays", pubkey, relayFilter)
            outboxRouter.subscribeToUserWriteRelays("userpins", pubkey, pinFilter)
            outboxRouter.subscribeToUserWriteRelays("usergroups", pubkey, groupsFilter)
            outboxRouter.subscribeToUserWriteRelays("userpaytargets", pubkey, payTargetsFilter)
        } else {
            relayPool.sendToAll(ClientMessage.req("userprofile", profileFilter))
            relayPool.sendToAll(ClientMessage.req("userposts", postsFilter))
            relayPool.sendToAll(ClientMessage.req("usergallery", galleryFilter))
            relayPool.sendToAll(ClientMessage.req("userfollows", followFilter))
            relayPool.sendToAll(ClientMessage.req("userrelays", relayFilter))
            relayPool.sendToAll(ClientMessage.req("userpins", pinFilter))
            relayPool.sendToAll(ClientMessage.req("usergroups", groupsFilter))
            relayPool.sendToAll(ClientMessage.req("userpaytargets", payTargetsFilter))
        }
        // Also query top scored relays as safety net
        for (url in topRelayUrls) {
            relayPool.sendToRelayOrEphemeral(url, ClientMessage.req("userposts", postsFilter))
            relayPool.sendToRelayOrEphemeral(url, ClientMessage.req("usergallery", galleryFilter))
            relayPool.sendToRelayOrEphemeral(url, ClientMessage.req("userprofile", profileFilter))
            relayPool.sendToRelayOrEphemeral(url, ClientMessage.req("userfollows", followFilter))
        }
        // Query indexer relays for groups list (kind 10009)
        _groupsLoading.value = true
        for (url in RelayConfig.DEFAULT_INDEXER_RELAYS) {
            relayPool.sendToRelayOrEphemeral(url, ClientMessage.req("usergroups", groupsFilter))
        }

        // NIP-85 follower count assertion
        loadFollowerCount(relayPool)

        // After posts EOSE, subscribe for engagement data
        if (subManager != null) {
            viewModelScope.launch {
                withTimeoutOrNull(15_000) {
                    relayPool.eoseSignals.first { it == "userposts" }
                }
                subscribeEngagementForProfile(relayPool)
            }
        }

        // Stop groups loading after EOSE or timeout
        viewModelScope.launch {
            withTimeoutOrNull(8_000) {
                relayPool.eoseSignals.first { it == "usergroups" }
            }
            _groupsLoading.value = false
        }

        viewModelScope.launch {
            relayPool.relayEvents.collect { (event, relayUrl, subscriptionId) ->
                // Track relay hints for all events we see
                relayHintStore?.addAuthorRelay(event.pubkey, relayUrl)
                relayHintStore?.extractHintsFromTags(event)
                // Refresh hints for the profile being viewed
                if (event.pubkey == pubkey) {
                    _relayHints.value = relayHintStore?.getHints(pubkey) ?: emptySet()
                }
                // Only process events from our own subscriptions
                if (subscriptionId !in SUB_IDS && !subscriptionId.startsWith("followprofiles") && !subscriptionId.startsWith("user-engage") && subscriptionId != "userposts-more" && subscriptionId != "userreplies-more") return@collect

                // Route engagement events — reactions, zaps, reply counts
                if (subscriptionId.startsWith("user-engage")) {
                    when (event.kind) {
                        7 -> eventRepo.addEvent(event)
                        9735 -> eventRepo.addEvent(event)
                        1 -> {
                            val parentId = Nip10.getReplyTarget(event)
                            if (parentId != null) eventRepo.addReplyCount(parentId, event.id)
                        }
                    }
                    return@collect
                }

                if (event.kind == 10002 && event.pubkey == pubkey) {
                    relayListRepo?.updateFromEvent(event)
                }
                if (event.kind == NipA3.KIND && event.pubkey == pubkey) {
                    paymentTargetRepoRef?.updateFromEvent(event)
                    _paymentTargets.value =
                        paymentTargetRepoRef?.getTargets(pubkey) ?: NipA3.parse(event)
                }
                if (event.pubkey == pubkey) {
                    when (event.kind) {
                        0 -> {
                            if (event.created_at > latestProfileTimestamp) {
                                latestProfileTimestamp = event.created_at
                                eventRepo.cacheEvent(event)
                                _profile.value = eventRepo.getProfileData(pubkey)
                            }
                        }
                        1 -> {
                            eventRepo.cacheEvent(event)
                            if (Nip10.getReplyTarget(event) == null) {
                                if (event.created_at < oldestNoteTimestamp) oldestNoteTimestamp = event.created_at
                                val current = _rootNotes.value.toMutableList()
                                if (current.none { it.id == event.id }) {
                                    current.add(event)
                                    current.sortByDescending { _repostSortTime[it.id] ?: it.created_at }
                                    _rootNotes.value = current
                                }
                            } else {
                                if (event.created_at < oldestReplyTimestamp) oldestReplyTimestamp = event.created_at
                                val current = _replies.value.toMutableList()
                                if (current.none { it.id == event.id }) {
                                    current.add(event)
                                    current.sortByDescending { it.created_at }
                                    _replies.value = current
                                }
                            }
                        }
                        30023, 1068, 6969 -> {
                            eventRepo.cacheEvent(event)
                            if (event.created_at < oldestNoteTimestamp) oldestNoteTimestamp = event.created_at
                            val current = _rootNotes.value.toMutableList()
                            if (current.none { it.id == event.id }) {
                                current.add(event)
                                current.sortByDescending { _repostSortTime[it.id] ?: it.created_at }
                                _rootNotes.value = current
                            }
                        }
                        20, 21, 22 -> {
                            eventRepo.cacheEvent(event)
                            if (event.created_at < oldestNoteTimestamp) oldestNoteTimestamp = event.created_at
                            // Add to gallery posts
                            val gallery = _galleryPosts.value.toMutableList()
                            if (gallery.none { it.id == event.id }) {
                                gallery.add(event)
                                gallery.sortByDescending { it.created_at }
                                _galleryPosts.value = gallery
                            }
                            // Also show in root notes feed
                            val current = _rootNotes.value.toMutableList()
                            if (current.none { it.id == event.id }) {
                                current.add(event)
                                current.sortByDescending { _repostSortTime[it.id] ?: it.created_at }
                                _rootNotes.value = current
                            }
                        }
                        6 -> {
                            eventRepo.cacheEvent(event)
                            if (event.created_at < oldestNoteTimestamp) oldestNoteTimestamp = event.created_at
                            // Show the reposted event in profile's root notes
                            if (event.content.isNotBlank()) {
                                try {
                                    val inner = NostrEvent.fromJson(event.content)
                                    eventRepo.cacheEvent(inner)
                                    _repostAuthors[inner.id] = event.pubkey
                                    _repostSortTime[inner.id] = event.created_at
                                    val current = _rootNotes.value.toMutableList()
                                    current.removeAll { it.id == inner.id }
                                    current.add(inner)
                                    current.sortByDescending { _repostSortTime[it.id] ?: it.created_at }
                                    _rootNotes.value = current
                                } catch (_: Exception) {}
                            }
                        }
                        3 -> {
                            if (event.created_at <= latestFollowListTimestamp) return@collect
                            latestFollowListTimestamp = event.created_at
                            val follows = Nip02.parseFollowList(event)
                            _followList.value = follows
                            // Request profiles for followed users we haven't cached
                            val uncached = follows
                                .map { it.pubkey }
                                .filter { eventRepo.getProfileData(it) == null }
                            if (uncached.isNotEmpty()) {
                                // Close any prior follow profile subs
                                for (subId in activeFollowProfileSubIds) relayPool.closeOnAllRelays(subId)
                                activeFollowProfileSubIds.clear()
                                // Chunk into batches to stay within relay filter limits
                                val batches = uncached.chunked(50)
                                batches.forEachIndexed { index, batch ->
                                    val subId = if (index == 0) "followprofiles" else "followprofiles-$index"
                                    activeFollowProfileSubIds.add(subId)
                                    val profileReq = Filter(
                                        kinds = listOf(0),
                                        authors = batch,
                                        limit = batch.size
                                    )
                                    relayPool.sendToAll(
                                        ClientMessage.req(subId, profileReq)
                                    )
                                }
                                // Close all followprofiles subs after EOSE or timeout
                                viewModelScope.launch {
                                    withTimeoutOrNull(15_000) {
                                        relayPool.eoseSignals.first { it == "followprofiles" }
                                    }
                                    for (subId in activeFollowProfileSubIds) {
                                        relayPool.closeOnAllRelays(subId)
                                    }
                                    activeFollowProfileSubIds.clear()
                                }
                            }
                        }
                        10001 -> {
                            _pinnedNoteIds.value = Nip51.parsePinList(event)
                        }
                        10002 -> {
                            if (event.created_at <= latestRelayListTimestamp) return@collect
                            latestRelayListTimestamp = event.created_at
                            _relayList.value = Nip65.parseRelayList(event)
                            // Re-subscribe now that we know the user's write relays
                            outboxRouterRef?.let { router ->
                                // Close old subs before re-subscribing
                                relayPool.closeOnAllRelays("userposts")
                                relayPool.closeOnAllRelays("userprofile")
                                relayPool.closeOnAllRelays("userfollows")
                                router.subscribeToUserWriteRelays("userposts", pubkey, postsFilter)
                                router.subscribeToUserWriteRelays("userprofile", pubkey, profileFilter)
                                router.subscribeToUserWriteRelays("userfollows", pubkey, followFilter)
                            }
                        }
                        Nip51.KIND_SIMPLE_GROUPS -> {
                            val parsed = Nip51.parseSimpleGroups(event)
                            if (parsed.isNotEmpty()) {
                                _groups.value = parsed
                            }
                            _groupsLoading.value = false
                        }
                    }
                } else if (event.kind == 0) {
                    // Profile for a followed user
                    eventRepo.cacheEvent(event)
                    _followProfileVersion.value++
                }
            }
        }
    }

    private fun subscribeEngagementForProfile(relayPool: RelayPool) {
        for (subId in activeEngagementSubIds) relayPool.closeOnAllRelays(subId)
        activeEngagementSubIds.clear()

        val eventIds = (_rootNotes.value.map { it.id } +
            _replies.value.map { it.id } +
            _sortedNotes.value.map { it.id } +
            _sortedReplies.value.map { it.id }).distinct()
        if (eventIds.isEmpty()) return

        // All events belong to targetPubkey — route to their inbox relays
        val router = outboxRouterRef
        if (router != null) {
            val eventsByAuthor = mapOf(targetPubkey to eventIds)
            router.subscribeEngagementByAuthors("user-engage", eventsByAuthor, activeEngagementSubIds)
        } else {
            // Fallback: no router available, use read relays
            eventIds.chunked(50).forEachIndexed { index, batch ->
                val subId = if (index == 0) "user-engage" else "user-engage-$index"
                activeEngagementSubIds.add(subId)
                val filters = listOf(
                    Filter(kinds = listOf(7), eTags = batch),
                    Filter(kinds = listOf(6), eTags = batch),
                    Filter(kinds = listOf(9735), eTags = batch),
                    Filter(kinds = listOf(1), eTags = batch)
                )
                relayPool.sendToReadRelays(ClientMessage.req(subId, filters))
            }
        }
        // Also query top scored relays for engagement
        eventIds.chunked(50).forEachIndexed { index, batch ->
            val subId = "user-engage-top-$index"
            activeEngagementSubIds.add(subId)
            val filters = listOf(
                Filter(kinds = listOf(7), eTags = batch),
                Filter(kinds = listOf(6), eTags = batch),
                Filter(kinds = listOf(9735), eTags = batch),
                Filter(kinds = listOf(1), eTags = batch)
            )
            for (url in topRelayUrls) {
                relayPool.sendToRelayOrEphemeral(url, ClientMessage.req(subId, filters))
            }
        }
    }

    private fun closeAllSubs(relayPool: RelayPool) {
        for (subId in SUB_IDS) {
            relayPool.closeOnAllRelays(subId)
        }
        relayPool.closeOnAllRelays("userposts-more")
        relayPool.closeOnAllRelays("userreplies-more")
        for (subId in activeFollowProfileSubIds) {
            relayPool.closeOnAllRelays(subId)
        }
        activeFollowProfileSubIds.clear()
        for (subId in activeEngagementSubIds) {
            relayPool.closeOnAllRelays(subId)
        }
        activeEngagementSubIds.clear()
        profileFeedNotesJob?.cancel()
        profileFeedNotesJob = null
        profileFeedRepliesJob?.cancel()
        profileFeedRepliesJob = null
        profileFollowersJob?.cancel()
        profileFollowersJob = null
        followerCountJob?.cancel()
        followerCountJob = null
    }

    override fun onCleared() {
        super.onCleared()
        relayPoolRef?.let { closeAllSubs(it) }
    }

    fun loadMoreNotes() {
        if (isLoadingMoreNotes) return
        isLoadingMoreNotes = true
        if (oldestNoteTimestamp == Long.MAX_VALUE) { isLoadingMoreNotes = false; return }

        val pool = relayPoolRef ?: run { isLoadingMoreNotes = false; return }
        val filter = Filter(kinds = listOf(1, 6, 1068, 6969, 30023, 20, 21, 22), authors = listOf(targetPubkey), until = oldestNoteTimestamp - 1, limit = 50)

        val router = outboxRouterRef
        if (router != null) {
            router.subscribeToUserWriteRelays("userposts-more", targetPubkey, filter)
        } else {
            pool.sendToAll(ClientMessage.req("userposts-more", filter))
        }
        for (url in topRelayUrls) {
            pool.sendToRelayOrEphemeral(url, ClientMessage.req("userposts-more", filter))
        }

        viewModelScope.launch {
            withTimeoutOrNull(10_000) {
                pool.eoseSignals.first { it == "userposts-more" }
            }
            pool.closeOnAllRelays("userposts-more")
            isLoadingMoreNotes = false
        }
    }

    fun loadMoreReplies() {
        if (isLoadingMoreReplies) return
        isLoadingMoreReplies = true
        if (oldestReplyTimestamp == Long.MAX_VALUE) { isLoadingMoreReplies = false; return }

        val pool = relayPoolRef ?: run { isLoadingMoreReplies = false; return }
        val filter = Filter(kinds = listOf(1), authors = listOf(targetPubkey), until = oldestReplyTimestamp - 1, limit = 50)

        val router = outboxRouterRef
        if (router != null) {
            router.subscribeToUserWriteRelays("userreplies-more", targetPubkey, filter)
        } else {
            pool.sendToAll(ClientMessage.req("userreplies-more", filter))
        }
        for (url in topRelayUrls) {
            pool.sendToRelayOrEphemeral(url, ClientMessage.req("userreplies-more", filter))
        }

        viewModelScope.launch {
            withTimeoutOrNull(10_000) {
                pool.eoseSignals.first { it == "userreplies-more" }
            }
            pool.closeOnAllRelays("userreplies-more")
            isLoadingMoreReplies = false
        }
    }

    fun setNotesSortMode(mode: ProfileSortMode) {
        _notesSortMode.value = mode
        if (mode == ProfileSortMode.RECENCY) {
            profileFeedNotesJob?.cancel()
            profileFeedNotesJob = null
            _sortedNotes.value = emptyList()
            _sortedNotesLoading.value = false
        } else {
            val url = "wss://feeds.nostrarchives.com/profiles/root/${mode.relaySlug()}"
            subscribeProfileRelayFeed(url, isReplies = false)
        }
    }

    fun setRepliesSortMode(mode: ProfileSortMode) {
        _repliesSortMode.value = mode
        if (mode == ProfileSortMode.RECENCY) {
            profileFeedRepliesJob?.cancel()
            profileFeedRepliesJob = null
            _sortedReplies.value = emptyList()
            _sortedRepliesLoading.value = false
        } else {
            val url = "wss://feeds.nostrarchives.com/profiles/replies/${mode.relaySlug()}"
            subscribeProfileRelayFeed(url, isReplies = true)
        }
    }

    /**
     * NIP-85 follower count: read the profile's kind 10040 to find the assertion
     * provider they trust for follower counts, then fetch that provider's kind
     * 30382 assertion about them. Falls back to the default provider relay when
     * no kind 10040 exists.
     */
    private fun loadFollowerCount(pool: RelayPool) {
        followerCountJob?.cancel()
        followerCountGen++
        val gen = followerCountGen
        val pubkey = targetPubkey
        val providerSubId = "nip85-provider-$gen"
        followerCountJob = viewModelScope.launch {
            try {
                var providerList: NostrEvent? = null
                val providerCollect = launch {
                    pool.relayEvents.collect { (event, _, subscriptionId) ->
                        if (subscriptionId != providerSubId) return@collect
                        if (event.kind != Nip85.KIND_PROVIDER_LIST || event.pubkey != pubkey) return@collect
                        if (event.created_at > (providerList?.created_at ?: 0L)) providerList = event
                    }
                }
                val providerFilter = Filter(kinds = listOf(Nip85.KIND_PROVIDER_LIST), authors = listOf(pubkey), limit = 1)
                val providerMsg = ClientMessage.req(providerSubId, providerFilter)
                val awaiting = mutableSetOf<Pair<String, String>>()
                outboxRouterRef?.subscribeToUserWriteRelays(providerSubId, pubkey, providerFilter)
                    ?.forEach { awaiting.add(providerSubId to it) }
                // Tiny replaceable-event REQ — also ask the whole pool and the
                // indexers so the list is found wherever it was published
                pool.sendToAll(providerMsg).forEach { awaiting.add(providerSubId to it) }
                for (url in RelayConfig.DEFAULT_INDEXER_RELAYS) {
                    if (pool.sendToRelayOrEphemeral(url, providerMsg)) awaiting.add(providerSubId to url)
                }
                awaitSubDone(pool, awaiting, 6_000)
                providerCollect.cancel()
                pool.closeOnAllRelays(providerSubId)

                val provider = providerList?.let { Nip85.parseProvider(it, Nip85.ASSERTION_FOLLOWERS) }
                Log.d("UserProfileVM", "nip85: 10040=${providerList != null} provider=${provider?.pubkey?.take(8)} hint=${provider?.relayHint}")

                var found = false
                if (provider != null) {
                    val relays = provider.relayHint?.let { listOf(it) } ?: profileRelayCandidates(pubkey)
                    found = fetchFollowerAssertion(pool, "nip85-followers-$gen-p", relays, provider.pubkey, pubkey, gen)
                }
                if (!found && followerCountGen == gen) {
                    // No provider list (or it yielded nothing): ask the profile's own
                    // relays and the whole pool — a relay running a NIP-85 provider
                    // serves its members' assertions locally
                    fetchFollowerAssertion(
                        pool, "nip85-followers-$gen-w", profileRelayCandidates(pubkey),
                        null, pubkey, gen, includeAllRelays = true
                    )
                }
            } finally {
                pool.closeOnAllRelays(providerSubId)
            }
        }
    }

    /** The profile's NIP-65 write relays, waiting briefly for a list still in flight. */
    private suspend fun awaitWriteRelays(pubkey: String, timeoutMs: Long = 6_000): List<String> {
        val repo = relayListRepoRef ?: return emptyList()
        val deadline = System.currentTimeMillis() + timeoutMs
        while (true) {
            val relays = repo.getWriteRelays(pubkey)
            if (!relays.isNullOrEmpty()) return relays
            if (System.currentTimeMillis() >= deadline) return emptyList()
            delay(500)
        }
    }

    /**
     * Wait until every (subscriptionId, relayUrl) pair in [awaiting] has sent
     * EOSE, [dataDone] completes, or [timeoutMs] passes. The timeout is only a
     * backstop for relays that die mid-subscription — completion normally comes
     * from the relays themselves. Returns false when the backstop fired.
     */
    private suspend fun awaitSubDone(
        pool: RelayPool,
        awaiting: Set<Pair<String, String>>,
        timeoutMs: Long,
        dataDone: CompletableDeferred<Unit>? = null
    ): Boolean {
        val outstanding = awaiting.toMutableSet()
        val finished = CompletableDeferred<Unit>()
        val watchers = mutableListOf<Job>()
        if (outstanding.isNotEmpty()) {
            watchers += viewModelScope.launch {
                pool.eoseDetails.first { pair ->
                    outstanding.remove(pair)
                    outstanding.isEmpty()
                }
                finished.complete(Unit)
            }
        }
        if (dataDone != null) {
            watchers += viewModelScope.launch {
                dataDone.await()
                finished.complete(Unit)
            }
        }
        if (watchers.isEmpty()) return false
        return try {
            withTimeoutOrNull(timeoutMs) { finished.await() } != null
        } finally {
            for (w in watchers) w.cancel()
        }
    }

    /**
     * Relays likely to hold the profile's data: NIP-65 write relays plus relays
     * their events have actually been seen on. All runtime-derived — no
     * hardcoded relay list.
     */
    private suspend fun profileRelayCandidates(pubkey: String): List<String> {
        val relays = LinkedHashSet<String>()
        relays.addAll(awaitWriteRelays(pubkey))
        relayHintStoreRef?.getHints(pubkey)?.let { relays.addAll(it) }
        return relays.take(8)
    }

    /**
     * Query relays for a kind 30382 follower assertion about [pubkey]. The newest
     * card wins; [authorPubkey] pins the accepted author when the profile named a
     * provider. True when a count was found.
     */
    private suspend fun fetchFollowerAssertion(
        pool: RelayPool,
        subId: String,
        relays: List<String>,
        authorPubkey: String?,
        pubkey: String,
        gen: Int,
        includeAllRelays: Boolean = false
    ): Boolean {
        if (relays.isEmpty() && !includeAllRelays) return false
        var latest = 0L
        val collectJob = viewModelScope.launch {
            pool.relayEvents.collect { (event, relayUrl, subscriptionId) ->
                if (subscriptionId != subId) return@collect
                if (followerCountGen != gen) return@collect
                if (authorPubkey != null && event.pubkey != authorPubkey) return@collect
                val count = Nip85.parseFollowerCount(event, pubkey) ?: return@collect
                if (event.created_at > latest) {
                    latest = event.created_at
                    _followerCount.value = count
                    _followerCountSource.value = relayUrl.removePrefix("wss://").removePrefix("ws://").trimEnd('/')
                }
            }
        }
        try {
            val filter = Filter(
                kinds = listOf(Nip85.KIND_ASSERTION),
                authors = authorPubkey?.let { listOf(it) },
                dTags = listOf(pubkey),
                limit = 1
            )
            val msg = ClientMessage.req(subId, filter)
            val awaiting = mutableSetOf<Pair<String, String>>()
            for (relay in relays) {
                if (pool.sendToRelayOrEphemeral(relay, msg, skipBadCheck = true)) awaiting.add(subId to relay)
            }
            if (includeAllRelays) pool.sendToAll(msg).forEach { awaiting.add(subId to it) }
            Log.d("UserProfileVM", "nip85: query relays=$relays all=$includeAllRelays author=${authorPubkey?.take(8) ?: "any"} sent=${awaiting.size}")
            awaitSubDone(pool, awaiting, 8_000)
        } finally {
            collectJob.cancel()
            pool.closeOnAllRelays(subId)
        }
        Log.d("UserProfileVM", "nip85: $subId -> found=${latest > 0L}")
        return latest > 0L
    }

    fun loadFollowers() {
        val pool = relayPoolRef ?: return
        val pubkey = targetPubkey

        // Serve the accumulated session cache instantly; a fresh-enough cache
        // skips the refresh entirely
        val cached = followerRepoRef?.get(pubkey).orEmpty()
        if (cached.isNotEmpty()) {
            _followers.value = resolveFollowerProfiles(cached)
            _followersLoading.value = false
            _followersError.value = false
            if (followerRepoRef?.isFresh(pubkey) == true) return
        } else {
            if (_followersLoading.value) return
            _followers.value = emptyList()
            _followersLoading.value = true
            _followersError.value = false
        }

        profileFollowersJob?.cancel()
        profileFollowersGen++
        val gen = profileFollowersGen
        // Live row-by-row updates only while there is nothing to show yet;
        // with a cache on screen the refresh is silent and lands as one update
        val liveUpdate = cached.isEmpty()

        profileFollowersJob = viewModelScope.launch {
            val working = LinkedHashSet<String>()
            val anyRelayAnswered = loadFollowersFromContactLists(pool, pubkey, gen, working, liveUpdate)
            if (profileFollowersGen != gen) return@launch

            // Union this run into the cache — a run that hit slow relays must
            // not shrink what a better run already found
            val merged = followerRepoRef?.merge(pubkey, working) ?: working
            fetchMissingProfiles(pool, gen, merged)
            if (profileFollowersGen != gen) return@launch
            if (merged.isNotEmpty()) {
                _followers.value = resolveFollowerProfiles(merged)
                _followersError.value = false
            } else {
                // Failure only when nothing was found AND no relay finished
                // responding; relays that answered with nothing stay the
                // regular empty state
                _followersError.value = _followers.value.isEmpty() && !anyRelayAnswered
            }
            _followersLoading.value = false
        }
    }

    /** Cached profile per pubkey, bare key row when none is known yet. */
    private fun resolveFollowerProfiles(pubkeys: Collection<String>): List<ProfileData> =
        pubkeys.map { pk ->
            eventRepoRef?.getProfileData(pk) ?: ProfileData(
                pubkey = pk, name = null, displayName = null, about = null,
                picture = null, banner = null, nip05 = null, lud16 = null,
                updatedAt = 0L
            )
        }

    /**
     * Follower pubkeys from kind 3 contact lists held by the profile's own
     * relays and the pool (a community relay that mirrors its members'
     * followers serves these locally). Fully decentralized — no fixed index
     * endpoint. Returns true when at least one relay finished responding.
     */
    private suspend fun loadFollowersFromContactLists(
        pool: RelayPool,
        pubkey: String,
        gen: Int,
        working: MutableSet<String>,
        liveUpdate: Boolean
    ): Boolean {
        val relays = profileRelayCandidates(pubkey)
        val k3SubId = "profile-followers-k3-$gen"
        // Whale guard: contact-list events are large, so stop the subscription
        // the moment the cap is reached instead of draining every relay
        val capReached = CompletableDeferred<Unit>()
        val k3Collect = viewModelScope.launch {
            pool.relayEvents.collect { (event, _, subscriptionId) ->
                if (subscriptionId != k3SubId) return@collect
                if (profileFollowersGen != gen) return@collect
                if (event.kind != 3 || !working.add(event.pubkey)) return@collect
                // First visit renders rows as they are found; refreshes under a
                // cached list land as one update at the end
                if (liveUpdate) {
                    _followers.value = _followers.value + resolveFollowerProfiles(listOf(event.pubkey))
                }
                if (working.size >= FollowerRepository.MAX_FOLLOWERS_PER_PROFILE) {
                    capReached.complete(Unit)
                }
            }
        }
        var anyEose = false
        val eoseWatch = viewModelScope.launch {
            pool.eoseDetails.first { it.first == k3SubId }
            anyEose = true
        }
        try {
            val filter = Filter(kinds = listOf(3), pTags = listOf(pubkey), limit = 500)
            val k3Msg = ClientMessage.req(k3SubId, filter)
            // One pass over the profile's relays and the whole pool together —
            // gating the pool on an empty first stage made results depend on
            // which slow subset happened to answer first
            val awaiting = mutableSetOf<Pair<String, String>>()
            for (relay in relays) {
                if (pool.sendToRelayOrEphemeral(relay, k3Msg, skipBadCheck = true)) awaiting.add(k3SubId to relay)
            }
            pool.sendToAll(k3Msg).forEach { awaiting.add(k3SubId to it) }
            Log.d("UserProfileVM", "followers: k3 queried=${awaiting.size} relays")
            awaitSubDone(pool, awaiting, 10_000, capReached)
        } finally {
            k3Collect.cancel()
            eoseWatch.cancel()
            pool.closeOnAllRelays(k3SubId)
        }
        Log.d("UserProfileVM", "followers: k3 done, total found=${working.size} anyEose=$anyEose")
        return anyEose || working.isNotEmpty()
    }

    /** Fetch kind 0 profiles for any pubkeys not in the event cache yet. */
    private suspend fun fetchMissingProfiles(pool: RelayPool, gen: Int, pubkeys: Collection<String>) {
        val missing = pubkeys.filter { eventRepoRef?.getProfileData(it) == null }
        if (missing.isEmpty()) return
        val k0SubId = "profile-followers-k0-$gen"
        val wanted = missing.toHashSet()
        val allResolved = CompletableDeferred<Unit>()
        val k0Collect = viewModelScope.launch {
            pool.relayEvents.collect { (event, _, subscriptionId) ->
                if (!subscriptionId.startsWith(k0SubId)) return@collect
                if (profileFollowersGen != gen) return@collect
                if (event.kind != 0 || !wanted.remove(event.pubkey)) return@collect
                eventRepoRef?.cacheEvent(event)
                if (wanted.isEmpty()) allResolved.complete(Unit)
            }
        }
        try {
            val awaiting = mutableSetOf<Pair<String, String>>()
            missing.chunked(50).forEachIndexed { index, batch ->
                val sub = if (index == 0) k0SubId else "$k0SubId-$index"
                val msg = ClientMessage.req(sub, Filter(kinds = listOf(0), authors = batch, limit = batch.size))
                // Bounded relay slice + indexers — a full pool broadcast per
                // chunk explodes into hundreds of REQs on whale profiles
                pool.sendToTopRelays(msg, maxRelays = 8).forEach { awaiting.add(sub to it) }
                for (url in RelayConfig.DEFAULT_INDEXER_RELAYS) {
                    if (pool.sendToRelayOrEphemeral(url, msg)) awaiting.add(sub to url)
                }
            }
            // Done when every profile resolved or every queried relay EOSE'd —
            // the timeout only backstops relays that never answer
            awaitSubDone(pool, awaiting, 12_000, allResolved)
            Log.d("UserProfileVM", "followers: k0 fetch unresolved=${wanted.size} of ${missing.size}")
        } finally {
            k0Collect.cancel()
            pool.closeOnAllRelays(k0SubId)
            val chunks = (missing.size + 49) / 50
            for (i in 1 until chunks) pool.closeOnAllRelays("$k0SubId-$i")
        }
    }

    private fun subscribeProfileRelayFeed(url: String, isReplies: Boolean) {
        val pool = relayPoolRef ?: return
        if (isReplies) {
            profileFeedRepliesGen++
            val gen = profileFeedRepliesGen
            val subId = "profile-feed-replies-$gen"
            profileFeedRepliesJob?.cancel()
            _sortedReplies.value = emptyList()
            _sortedRepliesLoading.value = true
            profileFeedRepliesJob = viewModelScope.launch { runProfileFeedSub(pool, url, subId, gen, isReplies = true) }
        } else {
            profileFeedNotesGen++
            val gen = profileFeedNotesGen
            val subId = "profile-feed-notes-$gen"
            profileFeedNotesJob?.cancel()
            _sortedNotes.value = emptyList()
            _sortedNotesLoading.value = true
            profileFeedNotesJob = viewModelScope.launch { runProfileFeedSub(pool, url, subId, gen, isReplies = false) }
        }
    }

    private suspend fun runProfileFeedSub(pool: RelayPool, url: String, subId: String, gen: Int, isReplies: Boolean) {
        val genCheck = { if (isReplies) profileFeedRepliesGen == gen else profileFeedNotesGen == gen }
        val setLoading = { v: Boolean -> if (isReplies) _sortedRepliesLoading.value = v else _sortedNotesLoading.value = v }
        val pubkey = targetPubkey
        val filter = Filter(kinds = listOf(1), authors = listOf(pubkey), limit = 100)

        var connected = false
        for (attempt in 0..2) {
            if (!genCheck()) { setLoading(false); return }
            if (attempt > 0) { pool.disconnectRelay(url); delay(1500L) }
            pool.sendToRelayOrEphemeral(url, ClientMessage.req(subId, filter), skipBadCheck = true)
            val deadline = System.currentTimeMillis() + 5_000
            while (System.currentTimeMillis() < deadline) {
                if (!genCheck()) { setLoading(false); return }
                if (pool.isRelayConnected(url)) { connected = true; break }
                delay(200)
            }
            if (connected) break
        }
        if (!connected) { setLoading(false); return }

        val seenIds = mutableSetOf<String>()
        val collectJob = viewModelScope.launch {
            pool.relayEvents.collect { (event, _, subscriptionId) ->
                if (subscriptionId != subId) return@collect
                if (!genCheck()) return@collect
                if (event.kind == 1 && seenIds.add(event.id)) {
                    eventRepoRef?.cacheEvent(event)
                    if (isReplies) {
                        val current = _sortedReplies.value.toMutableList()
                        current.add(event)
                        _sortedReplies.value = current
                    } else {
                        val current = _sortedNotes.value.toMutableList()
                        current.add(event)
                        _sortedNotes.value = current
                    }
                }
            }
        }

        // After 5 seconds with no events, stop showing the spinner so the empty message appears
        val timeoutJob = viewModelScope.launch {
            delay(5_000)
            if (genCheck()) setLoading(false)
        }

        val sm = subManagerRef
        if (sm != null) sm.awaitEoseCount(subId, 1)
        else withTimeoutOrNull(15_000) { pool.eoseSignals.first { it == subId } }
        collectJob.cancel()
        timeoutJob.cancel()
        setLoading(false)
        pool.closeOnAllRelays(subId)
        subscribeEngagementForProfile(pool)
    }

    fun toggleFollow(
        contactRepo: ContactRepository,
        relayPool: RelayPool,
        signer: NostrSigner? = null
    ) {
        val s = signer ?: keyRepo.getKeypair()?.let { LocalSigner(it.privkey, it.pubkey) } ?: return
        val currentList = contactRepo.getFollowList()
        val newList = if (contactRepo.isFollowing(targetPubkey)) {
            Nip02.removeFollow(currentList, targetPubkey)
        } else {
            Nip02.addFollow(currentList, targetPubkey)
        }

        val tags = Nip02.buildFollowTags(newList)
        viewModelScope.launch {
            val event = s.signEvent(kind = 3, content = "", tags = tags)
            relayPool.sendToWriteRelays(ClientMessage.event(event))
            contactRepo.updateFromEvent(event)
            _isFollowing.value = contactRepo.isFollowing(targetPubkey)
        }
    }
}
