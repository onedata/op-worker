%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2015 ACK CYFRONET AGH
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
    ?debug("~s", [gui_str:format("FILE_DATA_BACKEND: ~s: ~p", [??_Arg, _Arg])])
).


init() ->
    ?log_debug(websocket_init),
    {ok, _Pid} = data_backend:async_process(fun() -> async_loop() end),
    ok.

%% Currently unused
async_loop() ->
    receive
        Other ->
            ?log_debug({async_loop, Other})
    end,
    async_loop().


%% A virtual 'root' dir is introduces, that includes virtual dirs representing
%% spaces (they are given by id space#<space_id>.
%% This way, we can get all user spaces without having to list all the
%% children of the spaces.
%% In Ember, if a record has a relation, you have to return all the
%% IDs of related records. If we returned space dirs here, we would have to
%% list them all for file ids.
%% The virtual spaces returned here have one child each - the proper space dir.
find(<<"file">>, [<<"root">>]) ->
    SessionId = g_session:get_session_id(),
    {ok, SpaceDirs} = logical_file_manager:ls(SessionId,
        {path, <<"/spaces">>}, 1000, 0),
    VirtSpaceIds = [<<"space#", SpaceID/binary>> || {SpaceID, _} <- SpaceDirs],
    Res = [
        {<<"id">>, <<"root">>},
        {<<"name">>, <<"root">>},
        {<<"type">>, <<"dir">>},
        {<<"parentId">>, null},
        {<<"parent">>, null},
        {<<"children">>, VirtSpaceIds}
    ],
    ?log_debug({find_root, Res}),
    {ok, Res};


%% Called when ember asks for the virtual space dir -
%% it has one child, the proper space dir.
find(<<"file">>, [<<"space#", SpaceID/binary>> = VirtSpaceID]) ->
    SessionId = g_session:get_session_id(),
    {ok, #file_attr{name = Name}} = logical_file_manager:stat(
        SessionId, {uuid, SpaceID}),
    Res = [
        {<<"id">>, VirtSpaceID},
        {<<"name">>, Name},
        {<<"type">>, <<"dir">>},
        {<<"parentId">>, <<"root">>},
        {<<"parent">>, <<"root">>},
        {<<"children">>, [SpaceID]}
    ],
    ?log_debug({find_space, Res}),
    {ok, Res};


%% Called when ember asks for a file, dir or space dir
find(<<"file">>, [Id]) ->
    ?log_debug({find, <<"file">>, Id}),
    SessionId = g_session:get_session_id(),
    SpacesDirUUID = get_spaces_dir_uuid(),
    ParentUUID = case get_parent(Id) of
                     SpacesDirUUID ->
                         <<"space#", Id/binary>>;
                     Other ->
                         Other
                 end,
    {ok, #file_attr{name = Name, type = TypeAttr}} =
        logical_file_manager:stat(SessionId, {uuid, Id}),
    Type = case TypeAttr of
               ?DIRECTORY_TYPE -> <<"dir">>;
               _ -> <<"file">>
           end,
    % TODO for now, check the size by reading the file, because stat does not work
    Size = case Type of
               <<"dir">> ->
                   0;
               <<"file">> ->
                   {ok, Handle} = logical_file_manager:open(SessionId, {uuid, Id}, read),
                   {ok, _, Bytes} = logical_file_manager:read(Handle, 0, 1005),
                   byte_size(Bytes)
           end,
    Content = case Type =:= <<"file">> andalso Size < 1000 of
                  true ->
                      <<"content#", Id/binary>>;
                  _ ->
                      null
              end,
    Children = case Type of
                   <<"file">> ->
                       [];
                   <<"dir">> ->
                       {ok, Chldrn} = logical_file_manager:ls(
                           SessionId, {uuid, Id}, 1000, 0),
                       Chldrn
               end,
    ChildrenIds = [ChId || {ChId, _} <- Children],
    Res = [
        {<<"id">>, Id},
        {<<"name">>, Name},
        {<<"type">>, Type},
        {<<"content">>, Content},
        {<<"parentId">>, ParentUUID},
        {<<"parent">>, ParentUUID},
        {<<"children">>, ChildrenIds}
    ],
    ?log_debug({find, Res}),
    {ok, Res};

%% Called when ember asks for content of a file
find(<<"fileContent">>, [<<"content#", FileId/binary>> = Id]) ->
    ?log_debug({find, <<"fileContent">>, Id}),
    SessionId = g_session:get_session_id(),
    {ok, Handle} = logical_file_manager:open(SessionId, {uuid, FileId}, read),
    {ok, _, Bytes} = logical_file_manager:read(Handle, 0, 10000),
    Res = [
        {<<"id">>, Id},
        {<<"bytes">>, Bytes},
        {<<"file">>, FileId}
    ],
    {ok, Res};

%% Called when ember asks for multiple files
find(<<"file">>, Ids) ->
    Res = [begin {ok, File} = find(<<"file">>, [Id]), File end || Id <- Ids],
    ?log_debug({find_ds, Res}),
    {ok, Res}.

%% Called when ember asks for all files
find_all(<<"file">>) ->
    {ok, Root} = find(<<"file">>, [<<"root">>]),
    ChildrenIds = proplists:get_value(<<"children">>, Root),
    Children = [begin {ok, Ch} = find(<<"file">>, [ChId]), Ch end || ChId <- ChildrenIds],
    Res = [Root | Children],
    ?log_debug({find_all, Res}),
    {ok, Res}.
%%     SessionId = g_session:get_session_id(),
%%     {ok, SpaceDirs} = logical_file_manager:ls(SessionId,
%%         {path, <<"/spaces">>}, 10, 0),
%%     Res = lists:map(
%%         fun({Id, Name}) ->
%%             {ok, Children} = logical_file_manager:ls(
%%                 SessionId, {uuid, Id}, 10, 0),
%%             ChildrenIds = [ChId || {ChId, _} <- Children],
%%             Res = [
%%                 {<<"id">>, Id},
%%                 {<<"name">>, Name},
%%                 {<<"parentId">>, <<"root">>},
%%                 {<<"parent">>, null},
%%                 {<<"children">>, ChildrenIds}
%%             ]
%%         end, SpaceDirs
%%     ),
%%     ?log_debug({find_all, Res}),
%%     {ok, Res}.


%% Called when ember asks for file mathcing given query
find_query(<<"file">>, _Data) ->
    {error, not_iplemented}.

%% Called when ember asks to create a record
create_record(<<"file">>, Data) ->
    ?log_debug({create_record, <<"file">>, Data}),
    SessionId = g_session:get_session_id(),
    Name = proplists:get_value(<<"name">>, Data),
    Type = proplists:get_value(<<"type">>, Data),
    ProposedParentUUID = proplists:get_value(<<"parentId">>, Data, null),
    Path = case ProposedParentUUID of
               null ->
                   <<"/", Name/binary>>;
               _ ->
                   {ok, ParentPath} = logical_file_manager:get_file_path(
                       SessionId, ProposedParentUUID),
                   filename:join([ParentPath, Name])
           end,
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
    Content = case Type of
                  <<"file">> ->
                      <<"content#", FileId/binary>>;
                  <<"dir">> ->
                      null
              end,
    ParentUUID = get_parent(FileId),
    Res = [
        {<<"id">>, FileId},
        {<<"name">>, Name},
        {<<"type">>, Type},
        {<<"content">>, Content},
        {<<"parentId">>, ParentUUID},
        {<<"parent">>, ParentUUID},
        {<<"children">>, []}
    ],
    ?log_debug({create_record, Res}),
    {ok, Res}.

%% Called when ember asks to update a record
update_record(<<"file">>, _Id, _Data) ->
    {error, not_iplemented};

update_record(<<"fileContent">>, <<"content#", FileId/binary>>, Data) ->
    ?log_debug({update_record, <<"fileContent">>, <<"content#", FileId/binary>>, Data}),
    Bytes = proplists:get_value(<<"bytes">>, Data, <<>>),
    SessionId = g_session:get_session_id(),
    ok = logical_file_manager:truncate(SessionId, {uuid, FileId}, 0),
    {ok, Handle} = logical_file_manager:open(SessionId, {uuid, FileId}, write),
    {ok, _, _} = logical_file_manager:write(Handle, 0, Bytes),
    ok.

%% Called when ember asks to delete a record
delete_record(<<"file">>, Id) ->
    ok = rm_rf(Id).


% ------------------------------------------------------------
% Internal
% ------------------------------------------------------------

get_parent(UUID) ->
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
        {uuid, Id}, 1000, 0),
    lists:foreach(
        fun({ChId, _}) ->
            ok = rm_rf(ChId)
        end, Children),
    ok = logical_file_manager:unlink(SessionId, {uuid, Id}).


