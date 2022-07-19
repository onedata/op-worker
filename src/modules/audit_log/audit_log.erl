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
-export([append/2, browse/2]).
%% Iterator API
-export([iterator_start_index/0, iterator_get_next_batch/3]).


-type id() :: json_infinite_log_model:id().

-type entry_source() :: binary().    %% ?SYSTEM_ENTRY_SOURCE | ?USER_ENTRY_SOURCE
-type entry_severity() :: binary().  %% see ?ENTRY_SEVERITY_LEVELS

%% entry in a format suitable for external APIs (ready to be encoded to json)
%% #{
%%     <<"index">> := audit_log_browse_opts:index(),
%%     <<"timestamp">> := audit_log_browse_opts:timestamp_millis(),
%%     <<"source">> => entry_source()
%%     <<"severity">> := entry_severity(),
%%     <<"content">> := json_utils:json_map()
%% }
-type entry() :: json_utils:json_map().

-type append_request() :: #audit_log_append_request{}.

%% browse_result in a format suitable for external APIs (ready to be encoded to json)
%% #{
%%     <<"isLast">> := boolean(),
%%     <<"logEntries">> := [entry()]
%% }
-type browse_result() :: json_utils:json_map().

-export_type([id/0]).
-export_type([entry_source/0, entry_severity/0, entry/0]).
-export_type([append_request/0, browse_result/0]).


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


-spec append(id(), append_request()) -> ok | {error, term()}.
append(Id, #audit_log_append_request{
    severity = Severity,
    source = Source,
    content = Content
}) ->
    json_infinite_log_model:append(Id, maps_utils:put_if_defined(#{
        <<"severity">> => Severity,
        <<"content">> => Content
    }, <<"source">>, Source, ?SYSTEM_ENTRY_SOURCE)).


-spec browse(id(), audit_log_browse_opts:opts()) ->
    {ok, browse_result()} | errors:error().
browse(Id, BrowseOpts) ->
    case json_infinite_log_model:list_and_postprocess(Id, BrowseOpts, gen_listing_postprocessor()) of
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


-spec iterator_start_index() -> audit_log_browse_opts:index().
iterator_start_index() ->
    json_infinite_log_model:default_start_index(exclusive).


-spec iterator_get_next_batch(pos_integer(), id(), audit_log_browse_opts:index()) ->
    {ok, [entry()], audit_log_browse_opts:index()} | stop.
iterator_get_next_batch(BatchSize, Id, LastListedIndex) ->
    {ok, {ProgressMarker, Entries}} = json_infinite_log_model:list_and_postprocess(
        Id,
        #{start_from => {index_exclusive, LastListedIndex}, limit => BatchSize},
        gen_listing_postprocessor()
    ),

    case {Entries, ProgressMarker} of
        {[], done} -> stop;
        _ -> {ok, Entries, element(1, lists:last(Entries))}
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec gen_listing_postprocessor() -> json_infinite_log_model:listing_postprocessor(entry()).
gen_listing_postprocessor() ->
    fun({IndexBin, {Timestamp, Entry}}) ->
        Entry#{
            <<"index">> => IndexBin,
            <<"timestamp">> => Timestamp
        }
    end.
