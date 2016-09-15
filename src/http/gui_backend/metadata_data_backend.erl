%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements data_backend_behaviour and is used to synchronize
%%% the file metadata model used in Ember application.
%%% @end
%%%-------------------------------------------------------------------
-module(metadata_data_backend).
-author("Lukasz Opiola").

-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").
-include_lib("ctool/include/posix/errors.hrl").

%% API
-export([init/0, terminate/0]).
-export([find/2, find_all/1, find_query/2]).
-export([create_record/2, update_record/3, delete_record/2]).
-export([metadata_record/2]).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback init/0.
%% @end
%%--------------------------------------------------------------------
-spec init() -> ok.
init() ->
    ok.


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback terminate/0.
%% @end
%%--------------------------------------------------------------------
-spec terminate() -> ok.
terminate() ->
    ok.


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback find/2.
%% @end
%%--------------------------------------------------------------------
-spec find(ResourceType :: binary(), Id :: binary()) ->
    {ok, proplists:proplist()} | gui_error:error_result().
find(<<"file-metadata">>, FileId) ->
    SessionId = g_session:get_session_id(),
    try
        metadata_record(SessionId, FileId)
    catch T:M ->
        ?warning("Cannot get metadata for file (~p). ~p:~p", [
            FileId, T, M
        ]),
        gui_error:internal_server_error()
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback find_all/1.
%% @end
%%--------------------------------------------------------------------
-spec find_all(ResourceType :: binary()) ->
    {ok, [proplists:proplist()]} | gui_error:error_result().
find_all(<<"file-metadata">>) ->
    gui_error:report_error(<<"Not iplemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback find_query/2.
%% @end
%%--------------------------------------------------------------------
-spec find_query(ResourceType :: binary(), Data :: proplists:proplist()) ->
    {ok, proplists:proplist()} | gui_error:error_result().
find_query(<<"file-metadata">>, _Data) ->
    gui_error:report_error(<<"Not implemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback create_record/2.
%% @end
%%--------------------------------------------------------------------
-spec create_record(RsrcType :: binary(), Data :: proplists:proplist()) ->
    {ok, proplists:proplist()} | gui_error:error_result().
create_record(<<"file-metadata">>, Data) ->
    FileId = proplists:get_value(<<"file">>, Data),
    ok = update_record(<<"file-metadata">>, FileId, Data),
    metadata_record(g_session:get_session_id(), FileId).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback update_record/3.
%% @end
%%--------------------------------------------------------------------
-spec update_record(RsrcType :: binary(), Id :: binary(),
    Data :: proplists:proplist()) ->
    ok | gui_error:error_result().
update_record(<<"file-metadata">>, FileId, Data) ->
    SessionId = g_session:get_session_id(),
    case proplists:get_value(<<"basic">>, Data) of
        undefined ->
            ok;
        NewXattrs ->
            NewXattrsKeys = proplists:get_keys(NewXattrs),
            % Get current xattrs
            {ok, CurrentXattrs} = logical_file_manager:list_xattr(
                SessionId, {guid, FileId}, false, false
            ),
            KeysToBeRemoved = CurrentXattrs -- NewXattrsKeys,
            % Remove xattrs that no longer exist
            lists:foreach(
                fun(Key) ->
                    ok = logical_file_manager:remove_xattr(
                        SessionId, {guid, FileId}, Key
                    )
                end, KeysToBeRemoved),
            % Update all xattrs that were sent by the client
            lists:foreach(
                fun({K, V}) ->
                    ok = logical_file_manager:set_xattr(
                        SessionId, {guid, FileId}, #xattr{name = K, value = V}
                    )
                end, NewXattrs)
    end,
    case proplists:get_value(<<"json">>, Data) of
        undefined ->
            ok;
        JSON ->
            JSONMap = json_utils:decode_map(json_utils:encode(JSON)),
            ok = logical_file_manager:set_metadata(
                SessionId, {guid, FileId}, <<"json">>, JSONMap, []
            )
    end,
    case proplists:get_value(<<"rdf">>, Data) of
        undefined ->
            ok;
        RDF ->
            ok = logical_file_manager:set_metadata(
                SessionId, {guid, FileId}, <<"rdf">>, RDF, []
            )
    end,
    ok.


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback delete_record/2.
%% @end
%%--------------------------------------------------------------------
-spec delete_record(RsrcType :: binary(), Id :: binary()) ->
    ok | gui_error:error_result().
delete_record(<<"file-metadata">>, FileId) ->
    SessionId = g_session:get_session_id(),
    {ok, XattrKeys} = logical_file_manager:list_xattr(
        SessionId, {guid, FileId}, false, false
    ),
    lists:foreach(
        fun(Key) ->
            ok = logical_file_manager:remove_xattr(
                SessionId, {guid, FileId}, Key
            )
        end, XattrKeys),
    ok = logical_file_manager:set_metadata(
        SessionId, {guid, FileId}, <<"json">>, #{}, []
    ),
    ok = logical_file_manager:set_metadata(
        SessionId, {guid, FileId}, <<"rdf">>, undefined, []
    ),
    ok.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Constructs a metadata record for given FileId.
%% @end
%%--------------------------------------------------------------------
-spec metadata_record(SessionId :: binary(), FileId :: binary()) ->
    {ok, proplists:proplist()} | gui_error:error_result().
metadata_record(SessionId, FileId) ->
    {ok, XattrKeys} = logical_file_manager:list_xattr(
        SessionId, {guid, FileId}, false, false
    ),
    Basic = lists:map(
        fun(Key) ->
            {ok, #xattr{value = Value}} = logical_file_manager:get_xattr(
                SessionId, {guid, FileId}, Key, false
            ),
            {Key, Value}
        end, XattrKeys),
    BasicVal = case Basic of
        [] -> null;
        _ -> Basic
    end,
    {ok, JSON} = logical_file_manager:get_metadata(
        SessionId, {guid, FileId}, <<"json">>, [], false
    ),
    JSONVal = case JSON of
        #{} -> null;
        _ -> json_utils:decode(json_utils:encode_map(JSON))
    end,
    {ok, RDF} = logical_file_manager:get_metadata(
        SessionId, {guid, FileId}, <<"rdf">>, [], false
    ),
    RDFVal = case RDF of
        undefined -> null;
        _ -> RDF
    end,
    {ok, [
        {<<"id">>, FileId},
        {<<"file">>, FileId},
        {<<"basic">>, BasicVal},
        {<<"json">>, JSONVal},
        {<<"rdf">>, RDFVal}
    ]}.
