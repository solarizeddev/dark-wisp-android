package com.darkwisp.app.nostr

/**
 * NIP-85 Trusted Assertions.
 *
 * Kind 10040 is the user's list of trusted assertion providers, with tags like
 * `["30382:followers", "<provider pubkey>", "<relay hint>"]`. Kind 30382 is a
 * provider's assertion about a pubkey (`d` tag = subject pubkey), carrying
 * result tags such as `["followers", "<int>"]`.
 */
object Nip85 {
    const val KIND_PROVIDER_LIST = 10040
    const val KIND_ASSERTION = 30382

    /** Assertion tag name for follower counts in kind 30382 events. */
    const val ASSERTION_FOLLOWERS = "followers"

    data class Provider(val pubkey: String, val relayHint: String?)

    /** A relay to query for assertions; a non-null pubkey pins the accepted author. */
    data class ProviderRelay(val pubkey: String?, val relay: String)

    /** Tried in order when the profile's kind 10040 is absent or yields nothing. */
    val DEFAULT_PROVIDERS = listOf(
        // stack.solar community provider — relay-cards bot, cards live only on this relay
        ProviderRelay(
            "927f57b04121d7988e4febebad5226a016c44ddb946f90729f7be5d15b800f28",
            "wss://relay.stack.solar"
        ),
        ProviderRelay(null, "wss://nip85.nostr.band")
    )

    /** Provider the user trusts for [assertion] (e.g. "followers") per their kind 10040. */
    fun parseProvider(event: NostrEvent, assertion: String): Provider? {
        if (event.kind != KIND_PROVIDER_LIST) return null
        val tag = event.tags.firstOrNull {
            it.size >= 2 && it[0] == "$KIND_ASSERTION:$assertion" && it[1].isNotBlank()
        } ?: return null
        return Provider(
            pubkey = tag[1],
            relayHint = tag.getOrNull(2)?.trim()?.trimEnd('/')?.takeIf { it.isNotBlank() }
        )
    }

    /** Follower count asserted about [subjectPubkey], or null when the event carries none. */
    fun parseFollowerCount(event: NostrEvent, subjectPubkey: String): Int? {
        if (event.kind != KIND_ASSERTION) return null
        if (event.tags.none { it.size >= 2 && it[0] == "d" && it[1] == subjectPubkey }) return null
        return event.tags.firstOrNull { it.size >= 2 && it[0] == ASSERTION_FOLLOWERS }
            ?.get(1)?.toIntOrNull()?.takeIf { it >= 0 }
    }
}
