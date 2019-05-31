%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Common definitions and records used by modules responsible for
%%% harvesting metadata.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(HARVESTING_HRL).
-define(HARVESTING_HRL, 1).

% name of harvesting_stream OTP supervisor
-define(HARVESTING_STREAM_SUP, harvesting_stream_sup).

-define(DEFAULT_HARVESTING_SEQ, 0).

% macros for names of harvesting_streams OTP gen_servers
-define(MAIN_HARVESTING_STREAM(SpaceId),
    {main_harvesting_stream, SpaceId}).
-define(AUX_HARVESTING_STREAM(SpaceId, HarvesterId, IndexId),
    {aux_harvesting_stream, {SpaceId, HarvesterId, IndexId}}).

% harvesting_stream messages
-define(TAKEOVER_PROPOSAL(Name, Seq), {takeover_proposal, Name, Seq}).
-define(TAKEOVER_ACCEPTED, takeover_accepted).
-define(TAKEOVER_REJECTED(NewUntil), {takeover_rejected, NewUntil}).
-define(SPACE_REMOVED, space_removed).
-define(SPACE_UNSUPPORTED, space_unsupported).

% Exceptions and errors
-define(HARVESTING_DOC_NOT_FOUND_EXCEPTION(State),
    {harvesting_doc_not_found_exception, State}).

% Records

% state of harvesting_stream gen_server
-record(hs_state, {
    % common fields
    name :: harvesting_stream:name(),
    callback_module :: module(),
    destination = harvesting_destination:init() :: harvesting_destination:destination(),
    space_id :: od_space:id(),
    provider_id :: undefined | od_provider:id(),

    last_seen_seq = ?DEFAULT_HARVESTING_SEQ :: couchbase_changes:seq(),
    last_sent_max_stream_seq = ?DEFAULT_HARVESTING_SEQ :: couchbase_changes:seq(),
    last_persisted_seq = ?DEFAULT_HARVESTING_SEQ :: couchbase_changes:seq(),

    stream_pid :: undefined | pid(),

    mode = streaming :: harvesting_stream:mode(),
    backoff :: backoff:backoff(),
    error_log = <<"">> :: binary(),
    log_level = error :: warning | error | debug,

    batch = harvesting_batch:new_accumulator() :: harvesting_batch:accumulator() | harvesting_batch:batch(),
    % Couchbase aggregates all deleted docs in the beginning of a stream,
    % so these changes can be safely ignored. The flag is set to false when
    % meaningful changes have been reached (first non-deleted #custom_metadata{} doc).
    ignoring_deleted = true :: boolean(),

    until :: couchbase_changes:until(),

    last_harvest_timestamp = time_utils:system_time_seconds() :: non_neg_integer(),

    % fields used by main_harvesting_stream
    % destination with all indices, for which aux_harvesting_streams have been started
    aux_destination = harvesting_destination:init() :: harvesting_destination:destination()
}).

-endif.