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
%%% THIS IS A PROTOTYPE AND AN EXAMPLE OF IMPLEMENTATION.
%%% @end
%%%-------------------------------------------------------------------
-module(file_data_backend).
-author("Lukasz Opiola").
-author("Jakub Liput").

-compile([export_all]).

-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").

%% API
-export([init/0]).
-export([find/2, find_all/1, find_query/2]).
-export([create_record/2, update_record/3, delete_record/2]).

%% Convenience macro to log a debug level log dumping given variable.
-define(log_debug(_Arg),
    ?alert("~s", [str_utils:format("FILE_BACKEND: ~s: ~p", [??_Arg, _Arg])])
).


init() ->
    ?log_debug({websocket_init, g_session:get_session_id()}),
    ok.


%% Called when ember asks for a file (or a dir, which is treated as a file)
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
        logical_file_manager:stat(SessionId, {uuid, FileId}),
    {Type, Size} = case TypeAttr of
        ?DIRECTORY_TYPE -> {<<"dir">>, null};
        _ -> {<<"file">>, SizeAttr}
    end,
    Permissions = integer_to_binary(PermissionsAttr, 8),
    Children = case Type of
        <<"file">> ->
            [];
        <<"dir">> ->
            {ok, Chldrn} = logical_file_manager:ls(
                SessionId, {uuid, FileId}, 0, 1000),
            Chldrn
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
    ?log_debug({find, Res}),
    {ok, Res}.

%% Called when ember asks for all files - not implemented, because we don't
%% want to return all files...
find_all(<<"file">>) ->
    {error, not_iplemented}.


%% Called when ember asks for file mathcing given query
find_query(<<"file">>, _Data) ->
    {error, not_iplemented}.

%% Called when ember asks to create a record
create_record(<<"file">>, Data) ->
    try
        ?log_debug({create_record, <<"file">>, Data}),
        SessionId = g_session:get_session_id(),
        Name = proplists:get_value(<<"name">>, Data),
        case binary:match(Name, <<"nie">>) of
            {_, _} ->
                    gui_error:report_warning(<<"Names with 'nie' forbidden!">>);
            nomatch ->
                Type = proplists:get_value(<<"type">>, Data),
                ParentUUID = proplists:get_value(<<"parent">>, Data, null),
                {ok, ParentPath} = logical_file_manager:get_file_path(
                    SessionId, ParentUUID),
                Path = filename:join([ParentPath, Name]),
                ?log_debug(Path),
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
                    logical_file_manager:stat(SessionId, {uuid, FileId}),
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
                    {<<"parent">>, ParentUUID},
                    {<<"children">>, []}
                ],
                ?log_debug({create_record, Res}),
                {ok, Res}
        end
    catch _:_ ->
        gui_error:report_warning(<<"Failed to create new directory.">>)
    end.

%% Called when ember asks to update a record
update_record(<<"file">>, FileId, Data) ->
    try
        SessionId = g_session:get_session_id(),
%%    Rename = fun(_NewName) ->
%%        {ok, OldPath} = logical_file_manager:get_file_path(SessionId, FileId),
%%        DirName = filename:dirname(OldPath),
%%        {ok, OldPath} = logical_file_manager:mv(SessionId, FileId),
%%        ?dump(OldPath),
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
                        SessionId, {uuid, FileId}, Perms);
                false ->
                    gui_error:report_warning(<<"Cannot change permissions, "
                    "invalid octal value.">>)
            end
        end,
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
        ?dump({FileId, Data}),
        ok
    catch _:_ ->
        gui_error:report_warning(<<"Cannot change permissions.">>)
    end.

%% Called when ember asks to delete a record
delete_record(<<"file">>, Id) ->
    try
        rm_rf(Id)
    catch error:{badmatch, {error, eacces}} ->
        gui_error:report_warning(<<"Cannot remove file - access denied.">>)
    end.


% ------------------------------------------------------------
% Internal
% ------------------------------------------------------------

get_parent(UUID) ->
    ?dump({get_parent, UUID}),
    SessionId = g_session:get_session_id(),
    {ok, ParentUUID} = logical_file_manager:get_parent(
        SessionId, {uuid, UUID}),
    case logical_file_manager:get_file_path(SessionId, ParentUUID) of
        {ok, <<"/spaces">>} ->
            get_spaces_dir_uuid();
        _ ->
            ParentUUID
    end.


get_spaces_dir_uuid() ->
    SessionId = g_session:get_session_id(),
    {ok, #file_attr{uuid = SpacesDirUUID}} = logical_file_manager:stat(
        SessionId, {path, <<"/spaces">>}),
    SpacesDirUUID.


rm_rf(Id) ->
    SessionId = g_session:get_session_id(),
    {ok, Children} = logical_file_manager:ls(SessionId,
        {uuid, Id}, 0, 1000),
    lists:foreach(
        fun({ChId, _}) ->
            ok = rm_rf(ChId)
        end, Children),
    ok = logical_file_manager:unlink(SessionId, {uuid, Id}).