%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc 
%%% This module is responsible for storing audit log of QoS operations on files. 
%%% Audit log is stores entries for single QoS entry.
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
    report_file_synchronized/2, 
    report_file_failed/3, 
    destroy/1, 
    list/2
]).

%% datastore_model callbacks
-export([get_ctx/0]).

-type id() :: qos_entry:id().

-export_type([id/0]).


-define(CTX, #{model => ?MODULE}).

-define(LOG_MAX_SIZE, op_worker:get_env(qos_entry_audit_log_max_size, 1000)).
-define(LOG_EXPIRATION, op_worker:get_env(qos_entry_audit_log_expiration_seconds, 604800)).

%%%===================================================================
%%% API
%%%===================================================================

-spec create(id()) -> {ok, id()}.
create(Id) ->
    ok = datastore_infinite_log:create(?CTX, Id, #{
        size_pruning_threshold => ?LOG_MAX_SIZE,
        age_pruning_threshold => ?LOG_EXPIRATION
    }),
    {ok, Id}.


-spec report_file_synchronized(id(), file_id:file_guid()) -> ok.
report_file_synchronized(Id, FileGuid) ->
    Content = #{
        <<"status">> => <<"synchronized">>,
        <<"severity">> => <<"info">>,
        <<"fileId">> => FileGuid
    },
    append(Id, json_utils:encode(Content)).


-spec report_file_failed(id(), file_id:file_guid(), {error, term()}) -> ok.
report_file_failed(Id, FileGuid, Error) ->
    Content = #{
        <<"status">> => <<"failed">>,
        <<"severity">> => <<"error">>,
        <<"fileId">> => FileGuid,
        <<"error">> => maps:get(<<"description">>, errors:to_json(Error))
    },
    append(Id, json_utils:encode(Content)).


-spec append(id(), infinite_log:content()) -> ok.
append(Id, BinaryContent) ->
    case datastore_infinite_log:append(?CTX, Id, BinaryContent) of
        ok -> 
            ok;
        ?ERROR_NOT_FOUND ->
            {ok, Id} = create(Id),
            append(Id, BinaryContent);
        {error, _} = Error -> Error
    end.


-spec destroy(id()) -> ok | {error, term()}.
destroy(Id) ->
    datastore_infinite_log:destroy(?CTX, Id).


-spec list(id(), infinite_log_browser:listing_opts()) -> 
    {ok, [json_utils:json_map()], boolean()} | {error, term()}.
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
