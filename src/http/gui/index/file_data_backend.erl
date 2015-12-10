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

-include_lib("ctool/include/logging.hrl").
-include("modules/fslogic/fslogic_common.hrl").

%% API
-export([init/0]).
-export([find/1, find_all/0, find_query/1]).
-export([create_record/1, update_record/2, delete_record/1]).


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


%% ?dump({async_loop_push_in_5, self()}),
%% timer:sleep(5000),
%% ?dump({async_loop_push, self()}),
%% data_backend:push([
%% {<<"id">>, <<"dyna">>},
%% {<<"name">>, <<"Dynamiczny">>},
%% {<<"attribute">>, begin random:seed(now()), random:uniform(9999999) end},
%% {<<"selected">>, false}]),
%% %%     throw(erorroror),
%% async_loop().


find([Id]) ->
    ?dump({find, Id}),
    SessionID = g_session:get_session_id(),
    {ok, #file_attr{name = Name}} = logical_file_manager:stat(
        SessionID, {uuid, Id}),
    {ok, Children} = logical_file_manager:ls(SessionID, {uuid, Id}, 10, 0),
    ChildrenIds = [ChId || {ChId, _} <- Children],
    ParentUUID = get_parent(Id),
    Res = [
        {<<"id">>, Id},
        {<<"name">>, Name},
        {<<"parentId">>, ParentUUID},
        {<<"parent">>, ParentUUID},
        {<<"children">>, ChildrenIds}
    ],
    ?dump({find, Res}),
    {ok, Res};


find(Ids) ->
    ?dump({find, Ids}),
    Files = lists:foldl(
        fun(Id, Acc) ->
            case Acc of
                error ->
                    error;
                _ ->
                    case get_files_by(<<"id">>, Id) of
                        [File] -> Acc ++ [File];
                        _ -> error
                    end
            end
        end, [], Ids),
    case Files of
        error -> {error, <<"Files not found">>};
        _ -> {ok, Files}
    end.


find_all() ->
    SessionID = g_session:get_session_id(),
    ?dump(find_all),
    {ok, #file_attr{uuid = SpacesDirUUID}} = logical_file_manager:stat(
        SessionID, {path, <<"/spaces">>}),
    {ok, Children} = logical_file_manager:ls(SessionID,
        {uuid, SpacesDirUUID}, 10, 0),
    ChildrenIds = [ChId || {ChId, _} <- Children],
    Res = [
        {<<"id">>, SpacesDirUUID},
        {<<"name">>, <<"spaces">>},
        {<<"parentId">>, null},
        {<<"parent">>, null},
        {<<"children">>, ChildrenIds}
    ],
%%     Res = [begin {ok, R} = find([Id]), R end || {Id, _Name} <- Files],
    ?dump({find_all, [Res]}),
    {ok, [Res]}.


find_query(Data) ->
    ?dump({find_query, Data}),
    Result = lists:foldl(
        fun({Attr, Val}, Acc) ->
            get_files_by(Attr, Val, Acc)
        end, get_files(), Data),
    {ok, Result}.


create_record(Data) ->
    Id = integer_to_binary(begin random:seed(now()), random:uniform(9999999) end),
    DataWithId = [{<<"id">>, Id} | Data],
    save_files(get_files() ++ [DataWithId]),
    ?dump(get_files()),
    push_updated(DataWithId),
    {ok, DataWithId}.


update_record(Id, Data) ->
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

delete_record(Id) ->
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
    case UUID of
        <<"spaces">> ->
            null;
        _ ->
            {ok, ParentUUID} = logical_file_manager:get_parent(
                SessionID, {uuid, UUID}),
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

