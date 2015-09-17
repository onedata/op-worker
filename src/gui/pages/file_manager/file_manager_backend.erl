%%%-------------------------------------------------------------------
%%% @author lopiola
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 20. Aug 2015 15:25
%%%-------------------------------------------------------------------
-module(file_manager_backend).
-author("lopiola").

-compile([export_all]).

-include_lib("ctool/include/logging.hrl").

%% API
-export([page_init/0, websocket_init/0]).
-export([find/2, find_all/1, find_query/2, create_record/2, update_record/3, delete_record/2]).


-define(FILE_FIXTURES, [
    [{<<"id">>, <<"f1">>}, {<<"name">>, <<"File 1">>}, {<<"attribute">>, 82364234}, {<<"selected">>, false}],
    [{<<"id">>, <<"f2">>}, {<<"name">>, <<"Plik 2">>}, {<<"attribute">>, 451345134}, {<<"selected">>, false}],
    [{<<"id">>, <<"f3">>}, {<<"name">>, <<"Notatki 3">>}, {<<"attribute">>, 56892}, {<<"selected">>, false}],
    [{<<"id">>, <<"f4">>}, {<<"name">>, <<"Readme 4">>}, {<<"attribute">>, 124123567}, {<<"selected">>, false}],
    [{<<"id">>, <<"d1">>}, {<<"name">>, <<"Dir 1">>}, {<<"attribute">>, 567833}, {<<"selected">>, false}],
    [{<<"id">>, <<"d2">>}, {<<"name">>, <<"Folder 2">>}, {<<"attribute">>, 12475323}, {<<"selected">>, false}],
    [{<<"id">>, <<"d3">>}, {<<"name">>, <<"Katalog 3">>}, {<<"attribute">>, 34554444}, {<<"selected">>, false}],
    [{<<"id">>, <<"dyna">>}, {<<"name">>, <<"Dynamiczny">>}, {<<"attribute">>, 145}, {<<"selected">>, false}]
]).


page_init() ->
    serve_html.

websocket_init() ->
    ?dump(websocket_init),
    save_files(?FILE_FIXTURES),
    ?dump(opn_cowboy_bridge:get_socket_pid()),
    ok.


find(<<"file">>, [Id]) ->
    case get_files_by(<<"id">>, Id) of
        [File] -> {ok, [File]};
        _ -> {error, <<"File not found">>}
    end;


find(<<"file">>, Ids) ->
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


find_all(<<"file">>) ->
    {ok, get_files()}.


find_query(<<"file">>, Data) ->
    Result = lists:foldl(
        fun({Attr, Val}, Acc) ->
            get_files_by(Attr, Val, Acc)
        end, get_files(), Data),
    {ok, Result}.


create_record(<<"file">>, Data) ->
    Id = integer_to_list(begin random:seed(now()), random:uniform(9999999) end),
    DataWithId = [{<<"id">>, Id} | Data],
    save_files(get_files() ++ [DataWithId]),
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
            ok
    end.

delete_record(<<"file">>, Id) ->
    lists:filter(
        fun(File) ->
            not lists:member({<<"id">>, Id}, File)
        end, get_files()),
    % TODO A co jak nie OK?
    ok.


handle_info(update) ->
    Rand = (begin random:seed(now()), random:uniform(9999999) end),
    update_record(<<"file">>, <<"dyna">>, [{<<"attribute">>, Rand}]),
    get_files().

% ------------------------------------------------------------
%
% ------------------------------------------------------------

save_files(Files) ->
    put(files, Files).

get_files() ->
    get(files).

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

