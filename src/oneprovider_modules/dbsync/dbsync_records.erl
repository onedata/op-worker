%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module acts as configuration for records supported by dbsync worker.
%%       Each record that needs to be synchronized has to be supported by all
%%       methods in this module.
%% @end
%% ===================================================================
-module(dbsync_records).
-author("Rafal Slota").


-include("oneprovider_modules/dao/dao.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([doc_to_db/1, get_space_ctx/2]).


%% doc_to_db/1
%% ====================================================================
%% @doc Shall return database name for given db_document.
-spec doc_to_db(#db_document{}) -> string() | binary().
%% ====================================================================
doc_to_db(#db_document{record = #file{}}) ->
    ?FILES_DB_NAME;
doc_to_db(#db_document{record = #file_meta{}}) ->
    ?FILES_DB_NAME;
doc_to_db(#db_document{record = #available_blocks{}}) ->
    ?FILES_DB_NAME.

%% get_space_ctx/2
%% ====================================================================
%% @doc Shall return list of UUIDs and associated #space_info{} record for given document.
%%      This method should return as many UUID as possible in advance.
%% @end
-spec get_space_ctx(#db_document{}, Acc :: [binary()]) ->
    {ok, {UUIDs :: [binary()], #space_info{}}} | {error, Reason :: any()}.
%% ====================================================================
get_space_ctx(#db_document{uuid = "", record = #file{}}, []) ->
    {error, no_space};
get_space_ctx(#db_document{uuid = UUID, record = #file{extensions = Exts, parent = Parent}}, UUIDs) ->
    case lists:keyfind(?file_space_info_extestion, 1, Exts) of
        {?file_space_info_extestion, #space_info{} = SpaceInfo} ->
            {ok, {UUIDs, SpaceInfo}};
        false ->
            {ok, ParentDoc} = dao_lib:apply(vfs, get_file, [{uuid, Parent}], 1),
            get_space_ctx(ParentDoc, [UUID | UUIDs])
    end;
get_space_ctx(#db_document{uuid = UUID, record = #file_meta{}}, UUIDs) ->
    {ok, #db_document{} = FileDoc} = dao_lib:apply(dao_vfs, file_by_meta_id, [UUID], 1),
    get_space_ctx(FileDoc, [UUID | UUIDs]);
get_space_ctx(#db_document{uuid = UUID, record = #available_blocks{file_id = FileId}}, UUIDs) ->
    {ok, #db_document{} = FileDoc} = dao_lib:apply(dao_vfs, get_file, [{uuid, FileId}], 1),
    get_space_ctx(FileDoc, [UUID | UUIDs]).
