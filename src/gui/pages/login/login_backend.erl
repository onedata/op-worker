%%%-------------------------------------------------------------------
%%% @author lopiola
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 20. Aug 2015 15:25
%%%-------------------------------------------------------------------
-module(login_backend).
-author("lopiola").

-compile([export_all]).

-include_lib("ctool/include/logging.hrl").

%% API
-export([page_init/0, websocket_init/0]).
-export([find/2, find_all/1, find_query/2, create_record/2, update_record/3, delete_record/2]).


page_init() ->
    serve_html.


websocket_init() ->
    ok.


find(<<"file">>, Ids) ->
    ok.


find_all(<<"file">>) ->
    ok.


find_query(<<"file">>, Data) ->
    ok.


create_record(<<"file">>, Data) ->
    ok.


update_record(<<"file">>, Id, Data) ->
    ok.

delete_record(<<"file">>, Id) ->
    ok.
