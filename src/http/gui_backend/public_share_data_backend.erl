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
-export([file_record/2]).

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
        {<<"publicUrl">>, PublicURL}
    ]};
find(<<"file-public">>, FileId) ->
    try
        file_record(?GUEST_SESS_ID, FileId)
    catch T:M ->
        ?warning("Cannot get meta-data for file (~p). ~p:~p", [
            FileId, T, M
        ]),
        {ok, [{<<"id">>, FileId}, {<<"type">>, <<"broken">>}]}
    end;
find(<<"file-property-public">>, FileId) ->
    try
        metadata_data_backend:metadata_record(?GUEST_SESS_ID, FileId)
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
%% Constructs a file record from given FileId.
%% @end
%%--------------------------------------------------------------------
-spec file_record(SessionId :: binary(), FileId :: binary()) ->
    {ok, proplists:proplist()}.
file_record(SessionId, FileId) ->
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
                provider_id = ProviderId
            } = FileAttr,

            % Resolve parent guid of this file
            {ok, ParentGUID} = logical_file_manager:get_parent(
                SessionId, {guid, FileId}
            ),
            % If the parent guid is the same as file id, it means that it is
            % the root file of the share -> tell ember there is no parent.
            Parent = case ParentGUID of
                FileId -> null;
                GUID -> GUID
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
            ChildrenIds = [ChId || {ChId, _} <- Children],
            {ok, HasCustomMetadata} = logical_file_manager:has_custom_metadata(
                SessionId, {guid, FileId}
            ),
            Metadata = case HasCustomMetadata of
                false -> null;
                true -> FileId
            end,
            Res = [
                {<<"id">>, FileId},
                {<<"name">>, Name},
                {<<"type">>, Type},
                {<<"permissions">>, Permissions},
                {<<"modificationTime">>, ModificationTime},
                {<<"size">>, Size},
                {<<"parent">>, Parent},
                {<<"children">>, ChildrenIds},
                {<<"fileAcl">>, FileId},
                {<<"share">>, null},
                {<<"provider">>, ProviderId},
                {<<"filePropertyPublic">>, Metadata}
            ],
            {ok, Res}
    end.
