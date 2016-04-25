%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @author Jakub Liput
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

-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").

%% API
-export([init/0]).
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
%% {@link data_backend_behaviour} callback find/2.
%% @end
%%--------------------------------------------------------------------
-spec find(ResourceType :: binary(), Ids :: [binary()]) ->
    {ok, proplists:proplist()} | gui_error:error_result().
find(<<"file">>, [FileId]) ->
    SessionId = g_session:get_session_id(),
    SpacesDirUUID = get_spaces_dir_uuid(),
    ParentUUID = case get_parent(FileId) of
        SpacesDirUUID ->
            null;
        Other ->
            Other
    end,
    {ok, #file_attr{
        name = Name,
        type = TypeAttr,
        size = SizeAttr,
        mtime = ModificationTime,
        mode = PermissionsAttr}} =
        logical_file_manager:stat(SessionId, {guid, FileId}),
    {Type, Size} = case TypeAttr of
        ?DIRECTORY_TYPE -> {<<"dir">>, null};
        _ -> {<<"file">>, SizeAttr}
    end,
    Permissions = integer_to_binary(PermissionsAttr, 8),
    Children = case Type of
        <<"file">> ->
            [];
        <<"dir">> ->
            case logical_file_manager:ls(
                SessionId, {uuid, FileId}, 0, 1000) of
                {ok, Chldrn} ->
                    Chldrn;
                _ ->
                    []
            end
    end,
    ChildrenIds = [ChId || {ChId, _} <- Children],
    Res = [
        {<<"id">>, FileId},
        {<<"name">>, Name},
        {<<"type">>, Type},
        {<<"permissions">>, Permissions},
        {<<"modificationTime">>, ModificationTime},
        {<<"size">>, Size},
        {<<"parent">>, ParentUUID},
        {<<"children">>, ChildrenIds}
    ],
    {ok, Res}.


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback find_all/1.
%% @end
%%--------------------------------------------------------------------
-spec find_all(ResourceType :: binary()) ->
    {ok, proplists:proplist()} | gui_error:error_result().
find_all(<<"file">>) ->
    gui_error:report_error(<<"Not iplemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback find_query/2.
%% @end
%%--------------------------------------------------------------------
-spec find_query(ResourceType :: binary(), Data :: proplists:proplist()) ->
    {ok, proplists:proplist()} | gui_error:error_result().
find_query(<<"file">>, _Data) ->
    gui_error:report_error(<<"Not iplemented">>).


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
        case binary:match(Name, <<"nie">>) of
            {_, _} ->
                gui_error:report_warning(<<"Names with 'nie' forbidden!">>);
            nomatch ->
                Type = proplists:get_value(<<"type">>, Data),
                ParentGUID = proplists:get_value(<<"parent">>, Data, null),
                {ok, ParentPath} = logical_file_manager:get_file_path(
                    SessionId, ParentGUID),
                Path = filename:join([ParentPath, Name]),
                FileId = case Type of
                    <<"file">> ->
                        {ok, FId} = logical_file_manager:create(
                            SessionId, Path, 8#777),
                        FId;
                    <<"dir">> ->
                        {ok, DirId} = logical_file_manager:mkdir(
                            SessionId, Path, 8#777),
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
                    {<<"children">>, []}
                ],
                {ok, Res}
        end
    catch _:_ ->
        gui_error:report_warning(<<"Failed to create new directory.">>)
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
        % @todo uncomment when mv is supported
%%    Rename = fun(_NewName) ->
%%        {ok, OldPath} = logical_file_manager:get_file_path(SessionId, FileId),
%%        DirName = filename:dirname(OldPath),
%%        {ok, OldPath} = logical_file_manager:mv(SessionId, FileId),
%%        ok
%%    end,
        Chmod = fun(NewPermsBin) ->
            Perms = case is_integer(NewPermsBin) of
                true ->
                    binary_to_integer(integer_to_binary(NewPermsBin), 8);
                false ->
                    binary_to_integer(NewPermsBin, 8)
            end,
            case Perms >= 0 andalso Perms =< 8#777 of
                true ->
                    ok = logical_file_manager:set_perms(
                        SessionId, {guid, FileId}, Perms);
                false ->
                    gui_error:report_warning(<<"Cannot change permissions, "
                    "invalid octal value.">>)
            end
        end,
        % @todo uncomment when mv is supported
%%    case proplists:get_value(<<"name">>, Data, undefined) of
%%        undefined ->
%%            ok;
%%        NewName ->
%%            Rename(NewName)
%%    end,
        case proplists:get_value(<<"permissions">>, Data, undefined) of
            undefined ->
                ok;
            NewPerms ->
                Chmod(NewPerms)
        end,
        ok
    catch _:_ ->
        gui_error:report_warning(<<"Cannot change permissions.">>)
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback delete_record/2.
%% @end
%%--------------------------------------------------------------------
-spec delete_record(RsrcType :: binary(), Id :: binary()) ->
    ok | gui_error:error_result().
delete_record(<<"file">>, Id) ->
    try
        rm_rf(Id)
    catch error:{badmatch, {error, eacces}} ->
        gui_error:report_warning(<<"Cannot remove file - access denied.">>)
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
-spec get_parent(FileGUID :: binary()) -> binary().
get_parent(FileGUID) ->
    SessionId = g_session:get_session_id(),
    {ok, ParentGUID} = logical_file_manager:get_parent(
        SessionId, {guid, FileGUID}),
    case logical_file_manager:get_file_path(SessionId, ParentGUID) of
        {ok, <<"/spaces">>} ->
            get_spaces_dir_uuid();
        _ ->
            ParentGUID
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns the UUID of user's /spaces dir.
%% @end
%%--------------------------------------------------------------------
-spec get_spaces_dir_uuid() -> binary().
get_spaces_dir_uuid() ->
    SessionId = g_session:get_session_id(),
    {ok, #file_attr{guid = SpacesDirUUID}} = logical_file_manager:stat(
        SessionId, {path, <<"/spaces">>}),
    SpacesDirUUID.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Recursively deletes a directory and all its contents.
%% @end
%%--------------------------------------------------------------------
-spec rm_rf(Id :: binary()) -> ok | no_return().
rm_rf(Id) ->
    SessionId = g_session:get_session_id(),
    {ok, Children} = logical_file_manager:ls(SessionId,
        {uuid, Id}, 0, 1000),
    lists:foreach(
        fun({ChId, _}) ->
            ok = rm_rf(ChId)
        end, Children),
    ok = logical_file_manager:unlink(SessionId, {uuid, Id}).