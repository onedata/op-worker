%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(files_counter).
-author("Michal Wrzeszcz").


-behavior(tree_gatherer_behaviour).


-include("modules/tree_gatherer/tree_gatherer.hrl").
-include("modules/datastore/datastore_models.hrl").


%% API
-export([get_counters/1, update_size/2, increment_count/1]).

%% tree_gatherer_behaviour callbacks
-export([init_cache/1, merge/3, save/2]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1]).


-type ctx() :: datastore:ctx().
-type parameter() :: files_count | size_sum.
-type parameter_value() :: non_neg_integer().
-type values_map() :: #{
    files_count := non_neg_integer(),
    size_sum := non_neg_integer()
}.


-define(CTX, #{
    model => ?MODULE
}).


%%%===================================================================
%%% API
%%%===================================================================

-spec get_counters(file_id:file_guid()) -> {ok, values_map()}.
get_counters(Guid) ->
    tree_gatherer_pes_callback:call(#tree_gatherer_get_request{
        guid = Guid,
        handler_module = ?MODULE,
        parameters = [files_count, size_sum]
    }).


-spec update_size(file_id:file_guid(), non_neg_integer()) -> ok.
update_size(Guid, SizeDiff) ->
    tree_gatherer_pes_callback:call(#tree_gatherer_update_request{
        guid = Guid,
        handler_module = ?MODULE,
        diff_map = #{size_sum => SizeDiff}
    }).


-spec increment_count(file_id:file_guid()) -> ok.
increment_count(Guid) ->
    tree_gatherer_pes_callback:call(#tree_gatherer_update_request{
        guid = Guid,
        handler_module = ?MODULE,
        diff_map = #{files_count => 1}
    }).


%%%===================================================================
%%% tree_gatherer_behaviour callbacks
%%%===================================================================

-spec init_cache(file_id:file_guid()) -> {ok, values_map()} | {error, term()}.
init_cache(Guid) ->
    {Uuid, _SpaceId} = file_id:unpack_guid(Guid),
    case datastore_model:get(?CTX, Uuid) of
        {ok, #document{value = #files_counter{files_count = FilesCount, size_sum = SizeSum}}} ->
            {ok, #{files_count => FilesCount, size_sum => SizeSum}};
        {error, not_found} ->
            {ok, #{files_count => 0, size_sum => 0}};
        Error ->
            Error
    end.


-spec merge(parameter(), parameter_value(), parameter_value()) -> parameter_value().
merge(_, Value1, Value2) ->
    Value1 + Value2.


-spec save(file_id:file_guid(), values_map()) -> ok | {error, term()}.
save(Guid, #{files_count := FilesCount, size_sum := SizeSum}) ->
    {Uuid, _SpaceId} = file_id:unpack_guid(Guid),
    datastore_model:save(?CTX, #document{
        key = Uuid,
        value = #files_counter{files_count = FilesCount, size_sum = SizeSum}
    }).


%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

-spec get_ctx() -> ctx().
get_ctx() ->
    ?CTX.


-spec get_record_struct(datastore_model:record_version()) -> datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {files_count, integer},
        {size_sum, integer}
    ]}.
