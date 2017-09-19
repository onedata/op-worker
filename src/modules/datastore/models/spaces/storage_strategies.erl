%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% Helper module for space_strategies model. Implements functions for
%%% operating on storage_strategies field of #space_strategies record.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_strategies).
-author("Jakub Kudzia").

-include("modules/datastore/datastore_models.hrl").

%% API
-export([update_import_start_time/3, update_import_finish_time/3,
    update_last_update_start_time/3, update_last_update_finish_time/3,
    get_import_finish_time/2, get_import_start_time/2, get_last_update_start_time/2,
    get_last_update_finish_time/2]).


%%-------------------------------------------------------------------
%% @doc
%% Returns value of import_finish_time field in #storage_strategies
%% record associated with given StorageId in Strategies map.
%% @end
%%-------------------------------------------------------------------
-spec get_import_finish_time(storage:id(), map()) -> space_strategy:timestamp().
get_import_finish_time(StorageId, Strategies) ->
    get(StorageId, import_finish_time, Strategies).

%%-------------------------------------------------------------------
%% @doc
%% Returns value of import_start_time field in #storage_strategies
%% record associated with given StorageId in Strategies map.
%% @end
%%-------------------------------------------------------------------
-spec get_import_start_time(storage:id(), map()) -> space_strategy:timestamp().
get_import_start_time(StorageId, Strategies) ->
    get(StorageId, import_start_time, Strategies).

%%-------------------------------------------------------------------
%% @doc
%% Returns value of import_start_time field in #storage_strategies
%% record associated with given StorageId in Strategies map.
%% @end
%%-------------------------------------------------------------------
-spec get_last_update_start_time(storage:id(), map()) -> space_strategy:timestamp().
get_last_update_start_time(StorageId, Strategies) ->
    get(StorageId, last_update_start_time, Strategies).

%%-------------------------------------------------------------------
%% @doc
%% Returns value of import_start_time field in #storage_strategies
%% record associated with given StorageId in Strategies map.
%% @end
%%-------------------------------------------------------------------
-spec get_last_update_finish_time(storage:id(), map()) -> space_strategy:timestamp().
get_last_update_finish_time(StorageId, Strategies) ->
    get(StorageId, last_update_finish_time, Strategies).

%%-------------------------------------------------------------------
%% @doc
%% Returns value of import_start_time field in #storage_strategies
%% record associated with given StorageId in Strategies map.
%% @end
%%-------------------------------------------------------------------
-spec update_import_start_time(storage:id(), non_neg_integer(), map()) -> map().
update_import_start_time(StorageId,  Value, Strategies) ->
    update(StorageId, import_start_time, Value, Strategies).

%%-------------------------------------------------------------------
%% @doc
%% Updates value of import_finish_time field in #storage_strategies
%% record associated with given StorageId in Strategies map.
%% @end
%%-------------------------------------------------------------------
-spec update_import_finish_time(storage:id(), non_neg_integer(), map()) -> map().
update_import_finish_time(StorageId, Value, Strategies) ->
    update(StorageId, import_finish_time, Value, Strategies).

%%-------------------------------------------------------------------
%% @doc
%% Updates value of last_update_start_time field in #storage_strategies
%% record associated with given StorageId in Strategies map.
%% @end
%%-------------------------------------------------------------------
-spec update_last_update_start_time(storage:id(), non_neg_integer(), map()) -> map().
update_last_update_start_time(StorageId, Value, Strategies) ->
    update(StorageId, last_update_start_time, Value, Strategies).

%%-------------------------------------------------------------------
%% @doc
%% Updates value of last_update_finish_time field in #storage_strategies
%% record associated with given StorageId in Strategies map.
%% @end
%%-------------------------------------------------------------------
-spec update_last_update_finish_time(storage:id(), non_neg_integer(), map()) -> map().
update_last_update_finish_time(StorageId, Value, Strategies) ->
    update(StorageId, last_update_finish_time, Value, Strategies).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Returns value of field Key in #storage_strategies record associated
%% with given StorageId in Strategies map.
%% @end
%%-------------------------------------------------------------------
-spec get(storage:id(), atom(), map()) -> space_strategy:timestamp().
get(StorageId, Key, Strategies) ->
    SS = maps:get(StorageId, Strategies),
    case Key of
        import_start_time ->
            SS#storage_strategies.import_start_time;
        import_finish_time ->
            SS#storage_strategies.import_finish_time;
        last_update_start_time ->
            SS#storage_strategies.last_update_start_time;
        last_update_finish_time ->
            SS#storage_strategies.last_update_finish_time
    end.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Updates value of field Key in #storage_strategies record associated
%% with given StorageId in Strategies map.
%% @end
%%-------------------------------------------------------------------
-spec update(storage:id(), atom(), non_neg_integer(), map()) -> map().
update(StorageId, Key, Value, Strategies) ->
    OldSS = maps:get(StorageId, Strategies),
    NewSS = case Key of
        import_start_time ->
            OldSS#storage_strategies{import_start_time = Value};
        import_finish_time ->
            OldSS#storage_strategies{import_finish_time = Value};
        last_update_start_time ->
            OldSS#storage_strategies{last_update_start_time = Value};
        last_update_finish_time ->
            OldSS#storage_strategies{last_update_finish_time = Value}
    end,
    Strategies#{StorageId => NewSS}.

