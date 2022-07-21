%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Common backend for audit log implementations with well defined API.
%%% @end
%%%-------------------------------------------------------------------
-module(audit_log).
-author("Bartosz Walkowicz").

-include("modules/audit_log/audit_log.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("cluster_worker/include/modules/datastore/infinite_log.hrl").

%% CRUD API
-export([create/1, create/2, delete/1]).
-export([normalize_severity/1]).
-export([append/2, browse/2]).
%% Iterator API
-export([new_iterator/0, next_batch/3]).


-type id() :: json_infinite_log_model:id().

-type entry_source() :: binary().    %% ?SYSTEM_AUDIT_LOG_ENTRY_SOURCE | ?USER_AUDIT_LOG_ENTRY_SOURCE
-type entry_severity() :: binary().  %% see ?AUDIT_LOG_SEVERITY_LEVELS

%% entry() typespec (impossible to define in pure erlang due to binary keys):
%% #{
%%     <<"index">> := audit_log_browse_opts:index(),
%%     <<"timestamp">> := audit_log_browse_opts:timestamp_millis(),
%%     <<"source">> => entry_source()  // [default: system]
%%     <<"severity">> := entry_severity(),
%%     <<"content">> := json_utils:json_map()
%% }
-type entry() :: json_utils:json_map().

-type append_request() :: #audit_log_append_request{}.

%% browse_result() typespec (impossible to define in pure erlang due to binary keys):
%% #{
%%     <<"isLast">> := boolean(),
%%     <<"logEntries">> := [entry()]
%% }
-type browse_result() :: json_utils:json_map().

-opaque iterator() :: audit_log_browse_opts:index().

-export_type([id/0]).
-export_type([entry_source/0, entry_severity/0, entry/0]).
-export_type([append_request/0, browse_result/0]).
-export_type([iterator/0]).


%%%===================================================================
%%% CRUD API
%%%===================================================================


-spec create(infinite_log:log_opts()) -> {ok, id()} | {error, term()}.
create(Opts) ->
    json_infinite_log_model:create(Opts).


-spec create(id(), infinite_log:log_opts()) -> ok | {error, term()}.
create(Id, Opts) ->
    json_infinite_log_model:create(Id, Opts).


-spec delete(id()) -> ok | {error, term()}.
delete(Id) ->
    json_infinite_log_model:destroy(Id).


-spec normalize_severity(binary()) -> entry_severity().
normalize_severity(ProvidedSeverity) ->
    case lists:member(ProvidedSeverity, ?AUDIT_LOG_SEVERITY_LEVELS) of
        true -> ProvidedSeverity;
        false -> ?INFO_AUDIT_LOG_SEVERITY
    end.


-spec append(id(), append_request()) -> ok | {error, term()}.
append(Id, #audit_log_append_request{
    severity = Severity,
    source = Source,
    content = Content
}) ->
    json_infinite_log_model:append(Id, maps_utils:put_if_defined(#{
        <<"severity">> => Severity,
        <<"content">> => Content
    }, <<"source">>, Source, ?SYSTEM_AUDIT_LOG_ENTRY_SOURCE)).


-spec browse(id(), audit_log_browse_opts:opts()) ->
    {ok, browse_result()} | errors:error().
browse(Id, BrowseOpts) ->
    case json_infinite_log_model:list_and_postprocess(Id, BrowseOpts, fun listing_postprocessor/1) of
        {ok, {ProgressMarker, EntrySeries}} ->
            {ok, #{
                <<"logEntries">> => EntrySeries,
                <<"isLast">> => ProgressMarker =:= done
            }};
        {error, not_found} ->
            ?ERROR_NOT_FOUND
    end.


%%%===================================================================
%%% Iterator API
%%%===================================================================


-spec new_iterator() -> iterator().
new_iterator() ->
    json_infinite_log_model:default_start_index(exclusive).


-spec next_batch(pos_integer(), id(), iterator()) ->
    {ok, [entry()], iterator()} | stop.
next_batch(BatchSize, Id, LastListedIndex) ->
    {ok, {ProgressMarker, Entries}} = json_infinite_log_model:list_and_postprocess(
        Id,
        #{start_from => {index_exclusive, LastListedIndex}, limit => BatchSize},
        fun listing_postprocessor/1
    ),

    case {Entries, ProgressMarker} of
        {[], done} -> stop;
        _ -> {ok, Entries, maps:get(<<"index">>, lists:last(Entries))}
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec listing_postprocessor(json_infinite_log_model:entry()) -> entry().
listing_postprocessor({IndexBin, {Timestamp, Entry}}) ->
    Entry#{
        <<"index">> => IndexBin,
        <<"timestamp">> => Timestamp
    }.
