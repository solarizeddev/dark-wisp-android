package com.darkwisp.app.repo

import android.util.LruCache

/**
 * Session cache of follower pubkeys per profile. Relay responses for follower
 * queries are racy — different subsets answer within the window on every run —
 * so single runs are merged (union) into this cache and the UI renders the
 * accumulated set. The list can only grow within a session, never shrink
 * because one refresh happened to hit slow relays.
 */
class FollowerRepository {

    private class Entry {
        val followers = LinkedHashSet<String>()
        var updatedAt = 0L
    }

    private val cache = LruCache<String, Entry>(30)

    companion object {
        /** Bound per-profile memory and per-visit relay work for whale profiles. */
        const val MAX_FOLLOWERS_PER_PROFILE = 1000

        /** A refresh newer than this is not worth repeating on tab re-entry. */
        const val FRESH_WINDOW_MS = 60_000L
    }

    /** Accumulated follower pubkeys for [pubkey], in first-seen order. */
    fun get(pubkey: String): Set<String> =
        cache.get(pubkey)?.followers?.let { LinkedHashSet(it) } ?: emptySet()

    fun isFresh(pubkey: String): Boolean {
        val entry = cache.get(pubkey) ?: return false
        return entry.followers.isNotEmpty() &&
            System.currentTimeMillis() - entry.updatedAt < FRESH_WINDOW_MS
    }

    /** Union [fresh] into the cached set and return the merged result. */
    fun merge(pubkey: String, fresh: Collection<String>): Set<String> {
        val entry = cache.get(pubkey) ?: Entry().also { cache.put(pubkey, it) }
        for (pk in fresh) {
            if (entry.followers.size >= MAX_FOLLOWERS_PER_PROFILE) break
            entry.followers.add(pk)
        }
        entry.updatedAt = System.currentTimeMillis()
        return LinkedHashSet(entry.followers)
    }
}
