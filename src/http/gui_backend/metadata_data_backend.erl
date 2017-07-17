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
-behavior(data_backend_behaviour).
-author("Lukasz Opiola").

-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").
-include_lib("ctool/include/posix/errors.hrl").

%% API
-export([init/0, terminate/0]).
-export([find_record/2, find_all/1, query/2, query_record/2]).
-export([create_record/2, update_record/3, delete_record/2]).
-export([metadata_record/2, metadata_record/3]).

%%%===================================================================
%%% data_backend_behaviour callbacks
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
%% {@link data_backend_behaviour} callback find_record/2.
%% @end
%%--------------------------------------------------------------------
-spec find_record(ResourceType :: binary(), Id :: binary()) ->
    {ok, proplists:proplist()} | gui_error:error_result().
find_record(ModelType, ResourceId) ->
    SessionId = case ModelType of
        <<"file-property-public">> ->
            ?GUEST_SESS_ID;
        _ ->
            % covers: file-property, file-property-shared
            gui_session:get_session_id()
    end,
    try
        metadata_record(ModelType, SessionId, ResourceId)
    catch T:M ->
        ?warning("Cannot get metadata for file (~p). ~p:~p", [
            ResourceId, T, M
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
%% ModelType covers: file-property, file-property-shared.
find_all(_ModelType) ->
    gui_error:report_error(<<"Not implemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback query/2.
%% @end
%%--------------------------------------------------------------------
-spec query(ResourceType :: binary(), Data :: proplists:proplist()) ->
    {ok, [proplists:proplist()]} | gui_error:error_result().
%% ModelType covers: file-property, file-property-shared.
query(_ModelType, _Data) ->
    gui_error:report_error(<<"Not implemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback query_record/2.
%% @end
%%--------------------------------------------------------------------
-spec query_record(ResourceType :: binary(), Data :: proplists:proplist()) ->
    {ok, proplists:proplist()} | gui_error:error_result().
%% ModelType covers: file-property, file-property-shared.
query_record(_ModelType, _Data) ->
    gui_error:report_error(<<"Not implemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback create_record/2.
%% @end
%%--------------------------------------------------------------------
-spec create_record(RsrcType :: binary(), Data :: proplists:proplist()) ->
    {ok, proplists:proplist()} | gui_error:error_result().
create_record(<<"file-property-public">>, _Data) ->
    gui_error:report_error(<<"Not implemented">>);
%% ModelType covers: file-property, file-property-shared.
create_record(ModelType, Data) ->
    ResourceId = proplists:get_value(<<"file">>, Data),
    ok = update_record(ModelType, ResourceId, Data),
    metadata_record(ModelType, gui_session:get_session_id(), ResourceId).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback update_record/3.
%% @end
%%--------------------------------------------------------------------
-spec update_record(RsrcType :: binary(), ResourceId :: binary(),
    Data :: proplists:proplist()) ->
    ok | gui_error:error_result().
update_record(<<"file-property-public">>, _ResourceId, _Data) ->
    gui_error:report_error(<<"Not implemented">>);
%% ModelType covers: file-property, file-property-shared.
update_record(ModelType, ResourceId, Data) ->
    FileId = case ModelType of
        <<"file-property">> ->
            ResourceId;
        % Covers file-property-shared and file-property-public
        <<"file-property-", _/binary>> ->
            {_ShareId, FId} = op_gui_utils:association_to_ids(ResourceId),
            FId
    end,
    SessionId = gui_session:get_session_id(),
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
        null ->
            ok = logical_file_manager:remove_metadata(
                SessionId, {guid, FileId}, json
            );
        JSON ->
            JSONMap = case JSON of
                [] -> #{};
                _ -> json_utils:decode_map(json_utils:encode(JSON))
            end,
            ok = logical_file_manager:set_metadata(
                SessionId, {guid, FileId}, json, JSONMap, []
            )
    end,
    case proplists:get_value(<<"rdf">>, Data) of
        undefined ->
            ok;
        null ->
            ok = logical_file_manager:remove_metadata(
                SessionId, {guid, FileId}, rdf
            );
        RDF ->
            ok = logical_file_manager:set_metadata(
                SessionId, {guid, FileId}, rdf, RDF, []
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
delete_record(<<"file-property-public">>, _FileId) ->
    gui_error:report_error(<<"Not implemented">>);
%% ModelType can be one of: file-property, file-property-shared.
delete_record(ModelType, ResourceId) ->
    FileId = case ModelType of
        <<"file-property">> ->
            ResourceId;
        % Covers file-property-shared and file-property-public
        <<"file-property-", _/binary>> ->
            {_ShareId, FId} = op_gui_utils:association_to_ids(ResourceId),
            FId
    end,
    SessionId = gui_session:get_session_id(),
    {ok, XattrKeys} = logical_file_manager:list_xattr(
        SessionId, {guid, FileId}, false, false
    ),
    lists:foreach(
        fun(Key) ->
            ok = logical_file_manager:remove_xattr(
                SessionId, {guid, FileId}, Key
            )
        end, XattrKeys),
    ok = logical_file_manager:remove_metadata(
        SessionId, {guid, FileId}, json
    ),
    ok = logical_file_manager:remove_metadata(
        SessionId, {guid, FileId}, rdf
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
    metadata_record(<<"file-property">>, SessionId, FileId).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Constructs a metadata record for given ResourceId.
%% ModelType can be one of: file-property, file-property-shared or
%% file-property-public.
%% @end
%%--------------------------------------------------------------------
-spec metadata_record(ModelType :: binary(), SessionId :: binary(),
    ResourceId :: binary()) ->
    {ok, proplists:proplist()} | gui_error:error_result().
metadata_record(ModelType, SessionId, ResId) ->
    FileId = case ModelType of
        <<"file-property">> ->
            ResId;
        % Covers file-property-shared and file-property-public
        <<"file-property-", _/binary>> ->
            {_ShareId, FId} = op_gui_utils:association_to_ids(ResId),
            FId
    end,
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
    GetJSONResult = logical_file_manager:get_metadata(
        SessionId, {guid, FileId}, json, [], false
    ),
    JSONVal = case GetJSONResult of
        {error, ?ENOATTR} -> null;
        {ok, Map} when map_size(Map) =:= 0 -> <<"{}">>;
        {ok, JSON} -> json_utils:decode(json_utils:encode_map(JSON))
    end,
    GetRDFResult = logical_file_manager:get_metadata(
        SessionId, {guid, FileId}, rdf, [], false
    ),
    RDFVal = case GetRDFResult of
        {error, ?ENOATTR} -> null;
        {ok, RDF} -> RDF
    end,
    {ok, [
        {<<"id">>, ResId},
        {<<"file">>, ResId},
        {<<"basic">>, BasicVal},
        {<<"json">>, JSONVal},
        {<<"rdf">>, RDFVal}
    ]}.
