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
    {ok, Pid} = data_backend:aync_process(fun() -> async_loop() end),
    ensure_ets(self(), Pid),
    ok.


async_loop() ->
    receive
        {push, Data} ->
            data_backend:push(Data);
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
%% %%     throw(dupa),
%% async_loop().


find([Id]) ->
    case get_files_by(<<"id">>, Id) of
        [File] -> {ok, [File]};
        _ -> {error, <<"File not found">>}
    end;


find(Ids) ->
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
    {ok, get_files()}.


find_query(Data) ->
    Result = lists:foldl(
        fun({Attr, Val}, Acc) ->
            get_files_by(Attr, Val, Acc)
        end, get_files(), Data),
    {ok, Result}.


create_record(Data) ->
    Id = integer_to_list(begin random:seed(now()), random:uniform(9999999) end),
    DataWithId = [{<<"id">>, Id} | Data],
    save_files(get_files() ++ [DataWithId]),
    push_to_all(DataWithId),
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
            push_to_all(NewFile),
            ok
    end.

delete_record(Id) ->
    lists:filter(
        fun(File) ->
            not lists:member({<<"id">>, Id}, File)
        end, get_files()),
    % TODO A co jak nie OK?
    ok.


% ------------------------------------------------------------
%
% ------------------------------------------------------------

-define(FILE_ETS, file_ets).

ensure_ets(WSPid, AsyncPid) ->
    UserId = op_gui_utils:get_user_id(),
    case ets:info(?FILE_ETS) of
        undefined ->
            ets:new(?FILE_ETS, [named_table, public, set, {read_concurrency, true}]);
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


push_to_all(Data) ->
    Self = self(),
    UserId = op_gui_utils:get_user_id(),
    [{{clients, UserId}, Clients}] = ets:lookup(?FILE_ETS, {clients, UserId}),
    lists:foreach(
        fun({WSPid, AsyncPid}) ->
            case WSPid of
                Self ->
                    ok;
                _ ->
                    AsyncPid ! {push, Data}
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

