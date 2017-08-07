%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Creating and querying popularity view
%%% @end
%%%--------------------------------------------------------------------
-module(file_popularity_view).
-author("Tomasz Lichon").

-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("ctool/include/logging.hrl").

-define(POPULAR_FILE_OPEN_COUNT, 10).

%% API
-export([create_popularity_view/1, get_unpopular_files/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates view on space files capable of ordering files by their popularity
%% @end
%%--------------------------------------------------------------------
-spec create_popularity_view(od_space:id()) -> ok | {error, term()}.
create_popularity_view(SpaceId) ->
    ViewFunction =
        <<"function (doc, meta) {"
        "   if(doc['_record'] == 'file_popularity' && doc['space_id'] == '", SpaceId/binary , "') { "
        "      emit("
        "         [doc['open_count'], doc['last_open_time']],"
        "         [doc['file_uuid'], doc['space_id']]"
        "      );"
        "   }"
        "}">>,
    Ctx = model:make_disk_ctx(file_popularity:model_init()),
    couchbase_driver:save_spatial_view_doc(Ctx, SpaceId, ViewFunction).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Finds unpopular files in space
%% @end
%%--------------------------------------------------------------------
-spec get_unpopular_files(od_space:id()) -> [file_ctx:ctx()].
get_unpopular_files(SpaceId) ->
    Ctx = model:make_disk_ctx(file_popularity:model_init()),
    Options = [
        {spatial, true},
        {stale, false},
        {start_range, [0, null]},
        {end_range, [?POPULAR_FILE_OPEN_COUNT, null]}
    ],
    {ok, {Rows}} = couchbase_driver:query_view(Ctx, SpaceId, SpaceId, Options),
    lists:map(fun(Row) ->
        {<<"value">>, [FileUuid, SpaceId]} = lists:keyfind(<<"value">>, 1, Row),
        file_ctx:new_by_guid(fslogic_uuid:uuid_to_guid(FileUuid, SpaceId))
    end, Rows).

%%%===================================================================
%%% Internal functions
%%%===================================================================