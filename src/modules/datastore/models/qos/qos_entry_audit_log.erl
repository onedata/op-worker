%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc 
%%% This module is responsible for storing audit log of QoS operations on files. 
%%% The audit log stores logs concerning all files affected by a specific QoS entry.
%%% @end
%%%-------------------------------------------------------------------
-module(qos_entry_audit_log).
-author("Michal Stanisz").

-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("cluster_worker/include/modules/datastore/infinite_log.hrl").

%% API
-export([
    create/1,
    report_synchronization_started/2, 
    report_file_synchronized/2, 
    report_file_synchronization_failed/3, 
    destroy/1, 
    list/2
]).

%% datastore_model callbacks
-export([get_ctx/0]).

-type id() :: qos_entry:id().
-type is_last() :: boolean().

-export_type([id/0, is_last/0]).


-define(CTX, #{model => ?MODULE}).

-define(LOG_MAX_SIZE, op_worker:get_env(qos_entry_audit_log_max_size, 1000)).
-define(LOG_EXPIRATION, op_worker:get_env(qos_entry_audit_log_expiration_seconds, 604800)).

%%%===================================================================
%%% API
%%%===================================================================

-spec create(id()) -> ok | {error, term()}.
create(Id) ->
    datastore_infinite_log:create(?CTX, Id, #{
        size_pruning_threshold => ?LOG_MAX_SIZE,
        age_pruning_threshold => ?LOG_EXPIRATION
    }).


-spec report_synchronization_started(id(), file_id:file_guid()) -> ok | {error, term()}.
report_synchronization_started(Id, FileGuid) ->
    {ok, ObjectId} = file_id:guid_to_objectid(FileGuid),
    Content = #{
        <<"status">> => <<"synchronization_started">>,
        <<"severity">> => <<"info">>,
        <<"fileId">> => ObjectId
    },
    datastore_infinite_log:append(?CTX, Id, json_utils:encode(Content)).


-spec report_file_synchronized(id(), file_id:file_guid()) -> ok | {error, term()}.
report_file_synchronized(Id, FileGuid) ->
    {ok, ObjectId} = file_id:guid_to_objectid(FileGuid),
    Content = #{
        <<"status">> => <<"synchronized">>,
        <<"severity">> => <<"info">>,
        <<"fileId">> => ObjectId
    },
    datastore_infinite_log:append(?CTX, Id, json_utils:encode(Content)).


-spec report_file_synchronization_failed(id(), file_id:file_guid(), {error, term()}) -> ok | {error, term()}.
report_file_synchronization_failed(Id, FileGuid, Error) ->
    {ok, ObjectId} = file_id:guid_to_objectid(FileGuid),
    Content = #{
        <<"status">> => <<"synchronization_failed">>,
        <<"severity">> => <<"error">>,
        <<"fileId">> => ObjectId,
        <<"error">> => errors:to_json(Error)
    },
    datastore_infinite_log:append(?CTX, Id, json_utils:encode(Content)).


-spec destroy(id()) -> ok | {error, term()}.
destroy(Id) ->
    datastore_infinite_log:destroy(?CTX, Id).


-spec list(id(), infinite_log_browser:listing_opts()) -> 
    {ok, [json_utils:json_map()], is_last()} | {error, term()}.
list(Id, Opts) ->
    case datastore_infinite_log:list(?CTX, Id, Opts#{direction => ?FORWARD}) of
        {ok, {ProgressMarker, ListingResult}} -> 
            {ok, map_listing_result(ListingResult), ProgressMarker == done};
        {error, _} = Error -> 
            Error
    end.


%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    ?CTX.


%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec map_listing_result([{infinite_log:entry_index(), infinite_log:entry()}]) -> 
    [json_utils:json_map()].
map_listing_result(ListingResult) ->
    lists:map(fun({_Index, {Timestamp, EncodedContent}}) ->
        DecodedContent = json_utils:decode(EncodedContent),
        DecodedContent#{
            <<"timestamp">> => Timestamp
        }
    end, ListingResult).
