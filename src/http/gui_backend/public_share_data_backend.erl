%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @author Jakub Liput
%%% @author Tomasz Lichon
%%% @copyright (C) 2015-2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements data_backend_behaviour and is used to synchronize
%%% the file model used in Ember application.
%%% @end
%%%-------------------------------------------------------------------
-module(public_share_data_backend).
-author("Lukasz Opiola").
-author("Jakub Liput").
-author("Tomasz Lichon").

-include("modules/fslogic/fslogic_common.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include_lib("ctool/include/posix/acl.hrl").

%% API
-export([init/0, terminate/0]).
-export([find/2, find_all/1, find_query/2]).
-export([create_record/2, update_record/3, delete_record/2]).

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
find(<<"share-public">>, ShareId) ->
    {ok, #document{
        value = #share_info{
            name = Name,
            root_file_id = RootFileId,
            public_url = PublicURL
        }}} = share_logic:get(provider, ShareId),
    {ok, [
        {<<"id">>, ShareId},
        {<<"name">>, Name},
        {<<"file">>, RootFileId},
        {<<"containerDir">>, <<"containerDir.", ShareId/binary>>},
        {<<"publicUrl">>, PublicURL}
    ]};
find(<<"file-public">>, <<"containerDir.", ShareId/binary>>) ->
    UserAuth = op_gui_utils:get_user_auth(),
    {ok, #document{
        value = #share_info{
            name = Name,
            root_file_id = RootFileId
        }}} = share_logic:get(UserAuth, ShareId),
    FileId = fslogic_uuid:share_guid_to_guid(RootFileId),
    Res = [
        {<<"id">>, <<"containerDir.", ShareId/binary>>},
        {<<"name">>, Name},
        {<<"type">>, <<"dir">>},
        {<<"permissions">>, 0},
        {<<"modificationTime">>, 0},
        {<<"size">>, 0},
        {<<"parent">>, null},
        {<<"children">>, [op_gui_utils:ids_to_association(ShareId, FileId)]},
        {<<"fileAcl">>, null},
        {<<"share">>, null},
        {<<"provider">>, null},
        {<<"fileProperty">>, null}
    ],
    {ok, Res};

find(<<"file-public">>,AssocId) ->
    SessionId = g_session:get_session_id(),
    {ShareId, FileId} = op_gui_utils:association_to_ids(AssocId),
    case logical_file_manager:stat(SessionId, {guid, FileId}) of
        {error, ?ENOENT} ->
            gui_error:report_error(<<"No such file or directory.">>);
        {ok, FileAttr} ->
            #file_attr{
                name = Name,
                type = TypeAttr,
                size = SizeAttr,
                mtime = ModificationTime,
                mode = PermissionsAttr,
                shares = Shares,
                provider_id = ProviderId
            } = FileAttr,

            ParentUUID = case Shares of
                [ShareId] ->
                    % Check if this is the root dir
                    <<"containerDir.", ShareId/binary>>;
                [] ->
                    op_gui_utils:ids_to_association(
                        ShareId, get_parent(SessionId, FileId)
                    )
            end,

            {Type, Size} = case TypeAttr of
                ?DIRECTORY_TYPE -> {<<"dir">>, null};
                _ -> {<<"file">>, SizeAttr}
            end,
            Permissions = integer_to_binary((PermissionsAttr rem 1000), 8),
            Children = case Type of
                <<"file">> ->
                    [];
                <<"dir">> ->
                    case logical_file_manager:ls(
                        SessionId, {guid, FileId}, 0, 1000) of
                        {ok, List} ->
                            List;
                        _ ->
                            []
                    end
            end,
            ChildrenIds = [
                op_gui_utils:ids_to_association(ShareId, ChId) ||
                {ChId, _} <- Children
            ],
            {ok, HasCustomMetadata} = logical_file_manager:has_custom_metadata(
                SessionId, {guid, FileId}
            ),
            Metadata = case HasCustomMetadata of
                false -> null;
                true -> FileId
            end,
            Res = [
                {<<"id">>, AssocId},
                {<<"name">>, Name},
                {<<"type">>, Type},
                {<<"permissions">>, Permissions},
                {<<"modificationTime">>, ModificationTime},
                {<<"size">>, Size},
                {<<"parent">>, ParentUUID},
                {<<"children">>, ChildrenIds},
                {<<"fileAcl">>, FileId},
                {<<"share">>, null},
                {<<"provider">>, ProviderId},
                {<<"fileProperty">>, Metadata}
            ],
            {ok, Res}
    end;


find(<<"file-property-public">>, AssocId) ->
    SessionId = g_session:get_session_id(),
    {_, FileId} = op_gui_utils:association_to_ids(AssocId),
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
        {<<"id">>, AssocId},
        {<<"file">>, AssocId},
        {<<"basic">>, BasicVal},
        {<<"json">>, JSONVal},
        {<<"rdf">>, RDFVal}
    ]}.


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback find_all/1.
%% @end
%%--------------------------------------------------------------------
-spec find_all(ResourceType :: binary()) ->
    {ok, [proplists:proplist()]} | gui_error:error_result().
find_all(_) ->
    gui_error:report_error(<<"Not iplemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback find_query/2.
%% @end
%%--------------------------------------------------------------------
-spec find_query(ResourceType :: binary(), Data :: proplists:proplist()) ->
    {ok, proplists:proplist()} | gui_error:error_result().
find_query(_, _Data) ->
    gui_error:report_error(<<"Not implemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback create_record/2.
%% @end
%%--------------------------------------------------------------------
-spec create_record(RsrcType :: binary(), Data :: proplists:proplist()) ->
    {ok, proplists:proplist()} | gui_error:error_result().
create_record(_, _Data) ->
    gui_error:report_error(<<"Not implemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback update_record/3.
%% @end
%%--------------------------------------------------------------------
-spec update_record(RsrcType :: binary(), Id :: binary(),
    Data :: proplists:proplist()) ->
    ok | gui_error:error_result().
update_record(_, _Id, _Data) ->
    gui_error:report_error(<<"Not implemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback delete_record/2.
%% @end
%%--------------------------------------------------------------------
-spec delete_record(RsrcType :: binary(), Id :: binary()) ->
    ok | gui_error:error_result().
delete_record(_, _Id) ->
    gui_error:report_error(<<"Not implemented">>).


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns the UUID of parent of given file. This is needed because
%% spaces dir has two different UUIDs, should be removed when this is fixed.
%% @end
%%--------------------------------------------------------------------
-spec get_parent(SessionId :: binary(), FileGUID :: binary()) -> binary().
get_parent(SessionId, FileGUID) ->
    {ok, ParentGUID} = logical_file_manager:get_parent(SessionId, {guid, FileGUID}),
    ParentGUID.