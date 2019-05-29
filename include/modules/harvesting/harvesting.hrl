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
-define(TAKEOVER_PROPOSAL(HSI, Name, Seq),
    {takeover_proposal, HSI, Name, Seq}).
-define(TAKEOVER_ACCEPTED, takeover_accepted).
-define(TAKEOVER_REJECTED(NewUntil), {takeover_rejected, NewUntil}).
-define(SPACE_REMOVED, space_removed).
-define(SPACE_UNSUPPORTED, space_unsupported).

% Exceptions and errors
-define(HARVESTING_DOC_NOT_FOUND_EXCEPTION(State),
    {harvesting_doc_not_found_exception, State}).

% Records

% harvesting model identifier
-record(hid, {
    id :: od_space:id(),
    label :: main | {od_harvester:id(), od_harvester:index()}
}).

% state of harvesting_stream gen_server
-record(hs_state, {
    % common fields
    name :: harvesting_stream:name(),
    hi :: harvesting:hid(),
    callback_module :: module(),
    destination :: #{od_harvester:id() => [od_harvester:index()]},
    space_id :: od_space:id(),
    provider_id :: undefined | od_provider:id(),

    last_seen_seq = ?DEFAULT_HARVESTING_SEQ :: couchbase_changes:seq(),
    last_sent_max_stream_seq = ?DEFAULT_HARVESTING_SEQ :: couchbase_changes:seq(),
    last_persisted_seq = ?DEFAULT_HARVESTING_SEQ :: couchbase_changes:seq(),

    stream_pid :: undefined | pid(),

    mode = streaming :: harvesting_stream:mode(),
    backoff :: term(),

    batch = harvesting_batch:init() :: harvesting_batch:batch(),
    % we can ignore all the docs, until first not deleted
    % #custom_metadata doc arrives
    ignoring_deleted = true :: boolean(),

    until :: couchbase_changes:until(),

    last_harvest_timestamp = 0 :: non_neg_integer(),

    % fields used by main_harvesting_stream
    aux_destination = #{} :: #{od_harvester:id() => [od_harvester:index()]}
}).

-endif.