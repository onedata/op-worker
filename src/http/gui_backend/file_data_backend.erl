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
-module(file_data_backend).
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
find(<<"file">>, FileId) ->
    SessionId = g_session:get_session_id(),
    try
        file_record(SessionId, FileId)
    catch T:M ->
        ?warning("Cannot get file-meta for file (~p). ~p:~p", [
            FileId, T, M
        ]),
        {ok, [{<<"id">>, FileId}, {<<"type">>, <<"broken">>}]}
    end;
find(<<"file-acl">>, FileId) ->
    SessionId = g_session:get_session_id(),
    file_acl_record(SessionId, FileId).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback find_all/1.
%% @end
%%--------------------------------------------------------------------
-spec find_all(ResourceType :: binary()) ->
    {ok, [proplists:proplist()]} | gui_error:error_result().
find_all(<<"file">>) ->
    gui_error:report_error(<<"Not iplemented">>);
find_all(<<"file-acl">>) ->
    gui_error:report_error(<<"Not iplemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback find_query/2.
%% @end
%%--------------------------------------------------------------------
-spec find_query(ResourceType :: binary(), Data :: proplists:proplist()) ->
    {ok, proplists:proplist()} | gui_error:error_result().
find_query(<<"file">>, _Data) ->
    gui_error:report_error(<<"Not implemented">>);
find_query(<<"file-acl">>, _Data) ->
    gui_error:report_error(<<"Not implemented">>);
find_query(<<"file-distribution">>, [{<<"fileId">>, FileId}]) ->
    SessionId = g_session:get_session_id(),
    {ok, Distributions} = logical_file_manager:get_file_distribution(SessionId, {guid, FileId}),
    Res = lists:map(
        fun(#{<<"providerId">> := ProviderId, <<"blocks">> := Blocks}) ->
            BlocksList =
                case Blocks of
                    [] ->
                        [0, 0];
                    _ ->
                        lists:foldl(
                            fun([Offset, Size], Acc) ->
                                Acc ++ [Offset, Offset + Size]
                            end, [], Blocks)
                end,
            [
                {<<"id">>, op_gui_utils:ids_to_association(FileId, ProviderId)},
                {<<"fileId">>, FileId},
                {<<"provider">>, ProviderId},
                {<<"blocks">>, BlocksList}
            ]
        end, Distributions),
    {ok, Res}.


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback create_record/2.
%% @end
%%--------------------------------------------------------------------
-spec create_record(RsrcType :: binary(), Data :: proplists:proplist()) ->
    {ok, proplists:proplist()} | gui_error:error_result().
create_record(<<"file">>, Data) ->
    try
        SessionId = g_session:get_session_id(),
        Name = proplists:get_value(<<"name">>, Data),
        Type = proplists:get_value(<<"type">>, Data),
        ParentGUID = proplists:get_value(<<"parent">>, Data, null),
        {ok, ParentPath} = logical_file_manager:get_file_path(
            SessionId, ParentGUID),
        Path = filename:join([ParentPath, Name]),
        FileId = case Type of
            <<"file">> ->
                {ok, FId} = logical_file_manager:create(SessionId, Path),
                FId;
            <<"dir">> ->
                {ok, DirId} = logical_file_manager:mkdir(SessionId, Path),
                DirId
        end,
        {ok, #file_attr{
            name = Name,
            size = SizeAttr,
            mtime = ModificationTime,
            mode = PermissionsAttr}} =
            logical_file_manager:stat(SessionId, {guid, FileId}),
        Size = case Type of
            <<"dir">> -> null;
            _ -> SizeAttr
        end,
        Permissions = integer_to_binary(PermissionsAttr, 8),
        Res = [
            {<<"id">>, FileId},
            {<<"name">>, Name},
            {<<"type">>, Type},
            {<<"permissions">>, Permissions},
            {<<"modificationTime">>, ModificationTime},
            {<<"size">>, Size},
            {<<"parent">>, ParentGUID},
            {<<"children">>, []},
            {<<"fileAcl">>, FileId}
        ],
        {ok, Res}
    catch _:_ ->
        case proplists:get_value(<<"type">>, Data, <<"dir">>) of
            <<"dir">> ->
                gui_error:report_warning(<<"Failed to create new directory.">>);
            <<"file">> ->
                gui_error:report_warning(<<"Failed to create new file.">>)
        end
    end;
create_record(<<"file-acl">>, Data) ->
    Id = proplists:get_value(<<"file">>, Data),
    case update_record(<<"file-acl">>, Id, Data) of
        ok ->
            file_acl_record(?ROOT_SESS_ID, Id);
        Error ->
            Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback update_record/3.
%% @end
%%--------------------------------------------------------------------
-spec update_record(RsrcType :: binary(), Id :: binary(),
    Data :: proplists:proplist()) ->
    ok | gui_error:error_result().
update_record(<<"file">>, FileId, Data) ->
    try
        SessionId = g_session:get_session_id(),
        case proplists:get_value(<<"permissions">>, Data, undefined) of
            undefined ->
                ok;
            NewPerms ->
                Perms = case is_integer(NewPerms) of
                    true ->
                        binary_to_integer(integer_to_binary(NewPerms), 8);
                    false ->
                        binary_to_integer(NewPerms, 8)
                end,
                case Perms >= 0 andalso Perms =< 8#777 of
                    true ->
                        ok = logical_file_manager:set_perms(
                            SessionId, {guid, FileId}, Perms);
                    false ->
                        gui_error:report_warning(<<"Cannot change permissions, "
                        "invalid octal value.">>)
                end
        end
    catch _:_ ->
        gui_error:report_warning(<<"Cannot change permissions.">>)
    end;
update_record(<<"file-acl">>, FileId, Data) ->
    try
        SessionId = g_session:get_session_id(),
        Acl = acl_utils:json_to_acl(Data),
        case logical_file_manager:set_acl(SessionId, {guid, FileId}, Acl) of
            ok ->
                ok;
            {error, ?EACCES} ->
                gui_error:report_warning(<<"Cannot change ACL - access denied.">>)
        end
    catch _:_ ->
        gui_error:report_warning(<<"Cannot change ACL.">>)
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback delete_record/2.
%% @end
%%--------------------------------------------------------------------
-spec delete_record(RsrcType :: binary(), Id :: binary()) ->
    ok | gui_error:error_result().
delete_record(<<"file">>, FileId) ->
    SessionId = g_session:get_session_id(),
    case logical_file_manager:rm_recursive(SessionId, {guid, FileId}) of
        ok ->
            ok;
        {error, ?EACCES} ->
            gui_error:report_warning(
                <<"Cannot remove file or directory - access denied.">>)
    end;
delete_record(<<"file-acl">>, FileId) ->
    SessionId = g_session:get_session_id(),
    case logical_file_manager:remove_acl(SessionId, {guid, FileId}) of
        ok ->
            ok;
        {error, ?EACCES} ->
            gui_error:report_warning(
                <<"Cannot remove ACL - access denied.">>)
    end.


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


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns the UUID of user's root dir.
%% @end
%%--------------------------------------------------------------------
-spec get_user_root_dir_uuid(SessionId :: binary()) -> binary().
get_user_root_dir_uuid(SessionId) ->
    {ok, #file_attr{uuid = UserRootDirUUID}} = logical_file_manager:stat(
        SessionId, {path, <<"/">>}),
    UserRootDirUUID.


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
                shares = Shares} = FileAttr,

            UserRootDirUUID = get_user_root_dir_uuid(SessionId),
            ParentUUID = case get_parent(SessionId, FileId) of
                UserRootDirUUID ->
                    null;
                Other ->
                    Other
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
            % Currently only one share per file is allowed
            Share = case Shares of
                [] -> null;
                [ShareId] -> ShareId
            end,
            Res = [
                {<<"id">>, FileId},
                {<<"name">>, Name},
                {<<"type">>, Type},
                {<<"permissions">>, Permissions},
                {<<"modificationTime">>, ModificationTime},
                {<<"size">>, Size},
                {<<"parent">>, ParentUUID},
                {<<"children">>, ChildrenIds},
                {<<"fileAcl">>, FileId},
                {<<"share">>, Share}
            ],
            {ok, Res}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Constructs a file acl record for given FileId.
%% @end
%%--------------------------------------------------------------------
-spec file_acl_record(SessionId :: binary(), FileId :: binary()) ->
    {ok, proplists:proplist()}.
file_acl_record(SessionId, FileId) ->
    case logical_file_manager:get_acl(SessionId, {guid, FileId}) of
        {error, ?ENOENT} ->
            gui_error:report_error(<<"No such file or directory.">>);
        {error, ?ENOATTR} ->
            gui_error:report_error(<<"No ACL defined.">>);
        {error, ?EACCES} ->
            gui_error:report_error(<<"Cannot read ACL - access denied.">>);
        {ok, Acl} ->
            Res = acl_utils:acl_to_json(FileId, Acl),
            {ok, Res}
    end.
