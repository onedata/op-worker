%%%-------------------------------------------------------------------
%%% @author lopiola
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 20. Aug 2015 15:25
%%%-------------------------------------------------------------------
-module(file_data_backend).
-author("lopiola").

-compile([export_all]).

-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").

%% API
-export([init/0]).
-export([find/2, find_all/1, find_query/2]).
-export([create_record/2, update_record/3, delete_record/2]).


-define(FILE_FIXTURES, [
    [{<<"id">>, <<"f1">>}, {<<"name">>, <<"File 1">>}, {<<"attribute">>, 82364234}, {<<"selected">>, false}],
    [{<<"id">>, <<"f2">>}, {<<"name">>, <<"Plik 2">>}, {<<"attribute">>, 451345134}, {<<"selected">>, false}],
    [{<<"id">>, <<"f3">>}, {<<"name">>, <<"Notatki 3">>}, {<<"attribute">>, 56892}, {<<"selected">>, false}],
    [{<<"id">>, <<"f4">>}, {<<"name">>, <<"Readme 4">>}, {<<"attribute">>, 124123567}, {<<"selected">>, false}]
]).


init() ->
    ?dump(websocket_init),
    {ok, Pid} = data_backend:async_process(fun() -> async_loop() end),
    ensure_ets(self(), Pid),
    ok.


async_loop() ->
    receive
        {push_updated, Data} ->
            data_backend:push_updated(Data);
        {push_deleted, Data} ->
            data_backend:push_deleted(Data);
        Other ->
            ?dump({async_loop, Other})
    end,
    async_loop().


find(<<"file">>, [<<"root">>]) ->
    SessionID = g_session:get_session_id(),
    {ok, SpaceDirs} = logical_file_manager:ls(SessionID,
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
    ?dump({find_root, Res}),
    {ok, Res};


find(<<"file">>, [<<"space#", SpaceID/binary>> = VirtSpaceID]) ->
    SessionID = g_session:get_session_id(),
    {ok, #file_attr{name = Name}} = logical_file_manager:stat(
        SessionID, {uuid, SpaceID}),
    Res = [
        {<<"id">>, VirtSpaceID},
        {<<"name">>, Name},
        {<<"type">>, <<"dir">>},
        {<<"parentId">>, <<"root">>},
        {<<"parent">>, <<"root">>},
        {<<"children">>, [SpaceID]}
    ],
    ?dump({find_space, Res}),
    {ok, Res};


find(<<"file">>, [Id]) ->
    ?dump({find, Id}),
    SessionID = g_session:get_session_id(),
    {ok, #file_attr{uuid = SpacesDirUUID}} =
        logical_file_manager:stat(SessionID, {path, <<"/spaces">>}),
    ParentUUID = case get_parent(Id) of
                     SpacesDirUUID ->
                         <<"space#", Id/binary>>;
                     Other ->
                         Other
                 end,
    {ok, #file_attr{name = Name, type = TypeAttr, size = Size}} =
        logical_file_manager:stat(SessionID, {uuid, Id}),
    Type = case TypeAttr of
               ?DIRECTORY_TYPE -> <<"dir">>;
               _ -> <<"file">>
           end,
    Content = case Type =:= <<"file">> andalso Size < 1000 of
                  true ->
                      <<"content#", Id/binary>>;
                  _ ->
                      null
              end,
    {ok, Children} = logical_file_manager:ls(SessionID, {uuid, Id}, 1000, 0),
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
    ?dump({find, Res}),
    {ok, Res};

find(<<"fileContent">>, [<<"content#", FileId/binary>> = Id]) ->
    SessionID = g_session:get_session_id(),
    {ok, Handle} = logical_file_manager:open(SessionID, {uuid, FileId}, read),
    {ok, _, Bytes} = logical_file_manager:read(Handle, 0, 1000),
    ?dump(Bytes),
    Res = [
        {<<"id">>, Id},
        {<<"bytes">>, Bytes},
        {<<"file">>, FileId}
    ],
    {ok, Res};


find(<<"file">>, Ids) ->
    Res = [begin {ok, File} = find(<<"file">>, [Id]), File end || Id <- Ids],
    ?dump({find_ds, Res}),
    {ok, Res}.


find_all(<<"file">>) ->
    {ok, Root} = find(<<"file">>, [<<"root">>]),
    ChildrenIds = proplists:get_value(<<"children">>, Root),
    Children = [begin {ok, Ch} = find(<<"file">>, [ChId]), Ch end || ChId <- ChildrenIds],
    Res = [Root | Children],
    ?dump({find_all, Res}),
    {ok, Res}.
%%     SessionID = g_session:get_session_id(),
%%     {ok, SpaceDirs} = logical_file_manager:ls(SessionID,
%%         {path, <<"/spaces">>}, 10, 0),
%%     Res = lists:map(
%%         fun({Id, Name}) ->
%%             {ok, Children} = logical_file_manager:ls(
%%                 SessionID, {uuid, Id}, 10, 0),
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
%%     ?dump({find_all, Res}),
%%     {ok, Res}.


find_query(<<"file">>, Data) ->
    ?dump({find_query, Data}),
    Result = lists:foldl(
        fun({Attr, Val}, Acc) ->
            get_files_by(Attr, Val, Acc)
        end, get_files(), Data),
    {ok, Result}.


create_record(<<"file">>, Data) ->
    Id = integer_to_binary(begin random:seed(now()), random:uniform(9999999) end),
    DataWithId = [{<<"id">>, Id} | Data],
    save_files(get_files() ++ [DataWithId]),
    ?dump(get_files()),
    push_updated(DataWithId),
    {ok, DataWithId}.


update_record(<<"file">>, Id, Data) ->
    case get_files_by(<<"id">>, Id) of
        [] ->
            {error, <<"File not found">>};
        [File] ->
            NewFile = update_file(File, Data),
            NewFileList = lists:map(
                fun(FileInList) ->
                    case lists:member({<<"id">>, Id}, FileInList) of
                        false ->
                            FileInList;
                        true ->
                            NewFile
                    end
                end, get_files()),
            save_files(NewFileList),
            push_updated(NewFile),
            ok
    end.

delete_record(<<"file">>, Id) ->
    NewFiles = lists:filter(
        fun(File) ->
            not lists:member({<<"id">>, Id}, File)
        end, get_files()),
    save_files(NewFiles),
    % TODO A co jak nie OK?
    push_deleted(Id),
    ok.


% ------------------------------------------------------------
%
% ------------------------------------------------------------

get_parent(UUID) ->
    SessionID = g_session:get_session_id(),
    {ok, ParentUUID} = logical_file_manager:get_parent(
        SessionID, {uuid, UUID}),
    case ParentUUID of
        <<"spaces">> ->
            {ok, #file_attr{uuid = SpacesDirUUID}} = logical_file_manager:stat(
                SessionID, {path, <<"/spaces">>}),
            SpacesDirUUID;
        _ ->
            ParentUUID
    end.



-define(FILE_ETS, file_ets).

ensure_ets(WSPid, AsyncPid) ->
    UserId = op_gui_utils:get_user_id(),
    Self = self(),
    case ets:info(?FILE_ETS) of
        undefined ->
            spawn(
                fun() ->
                    ets:new(?FILE_ETS, [named_table, public, set, {read_concurrency, true}]),
                    Self ! created,
                    receive
                        die ->
                            ok
                    end
                end),
            receive created -> ok end;
        _ ->
            ok
    end,
    case ets:lookup(?FILE_ETS, {files, UserId}) of
        [] ->
            ets:insert(?FILE_ETS, {{files, UserId}, ?FILE_FIXTURES});
        _ ->
            ok
    end,
    case ets:lookup(?FILE_ETS, {clients, UserId}) of
        [] ->
            ets:insert(?FILE_ETS, {{clients, UserId}, [{WSPid, AsyncPid}]});
        [{{clients, UserId}, List}] ->
            ets:insert(?FILE_ETS, {{clients, UserId}, [{WSPid, AsyncPid} | List]})
    end.


push_updated(Data) ->
    push_to_all(push_updated, Data).


push_deleted(Data) ->
    push_to_all(push_deleted, Data).


push_to_all(Type, Data) ->
    Self = self(),
    UserId = op_gui_utils:get_user_id(),
    [{{clients, UserId}, Clients}] = ets:lookup(?FILE_ETS, {clients, UserId}),
    lists:foreach(
        fun({WSPid, AsyncPid}) ->
            case WSPid of
                Self ->
                    ok;
                _ ->
                    AsyncPid ! {Type, Data}
            end
        end, Clients).


save_files(Files) ->
    UserId = op_gui_utils:get_user_id(),
    ets:insert(?FILE_ETS, {{files, UserId}, Files}).

get_files() ->
    UserId = op_gui_utils:get_user_id(),
    [{{files, UserId}, Files}] = ets:lookup(?FILE_ETS, {files, UserId}),
    Files.

get_files_by(AttrName, AttrValue) ->
    get_files_by(AttrName, AttrValue, get_files()).

get_files_by(AttrName, AttrValue, FileList) ->
    lists:filter(
        fun(File) ->
            lists:member({AttrName, AttrValue}, File)
        end, FileList).

update_file(File, Data) ->
    lists:foldl(
        fun({Attr, Val}, CurrFile) ->
            lists:keyreplace(Attr, 1, CurrFile, {Attr, Val})
        end, File, Data).

