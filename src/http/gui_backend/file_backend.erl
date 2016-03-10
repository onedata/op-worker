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
-module(file_backend).
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
find(<<"file">>, [<<"space1rootDir">>]) ->
    Res = [
        {<<"id">>, <<"space1rootDir">>},
        {<<"name">>, <<"Space 1">>},
        {<<"type">>, <<"dir">>},
        {<<"parent">>, null},
        {<<"children">>, [
          <<"s1d1">>,
          <<"s1f1">>,
          <<"s1f2">>
        ]}
    ],
    {ok, Res};

find(<<"file">>, [<<"space2rootDir">>]) ->
    Res = [
        {<<"id">>, <<"space2rootDir">>},
        {<<"name">>, <<"Space 2">>},
        {<<"type">>, <<"dir">>},
        {<<"parent">>, null},
        {<<"children">>, [
          <<"s2d1">>,
          <<"s2d2">>,
          <<"s1f1">>
        ]}
    ],
    {ok, Res};

find(<<"file">>, [<<"s1d1">>]) ->
    Res = [
        {<<"id">>, <<"s1d1">>},
        {<<"name">>, <<"Katalog">>},
        {<<"type">>, <<"dir">>},
        {<<"parent">>, <<"space1rootDir">>},
        {<<"children">>, [
          <<"s1d1d1">>
        ]}
    ],
    {ok, Res};

find(<<"file">>, [<<"s1d1d1">>]) ->
    Res = [
        {<<"id">>, <<"s1d1d1">>},
        {<<"name">>, <<"Podrzedny">>},
        {<<"type">>, <<"dir">>},
        {<<"parent">>, <<"s1d1">>},
        {<<"children">>, [
          <<"s1d1d1f1">>
        ]}
    ],
    {ok, Res};

find(<<"file">>, [<<"s1d1d1f1">>]) ->
    Res = [
        {<<"id">>, <<"s1d1d1f1">>},
        {<<"name">>, <<"Gleboki Plik">>},
        {<<"type">>, <<"file">>},
        {<<"parent">>, <<"s1d1d1">>},
        {<<"children">>, []}
    ],
    {ok, Res};

find(<<"file">>, [<<"s1f1">>]) ->
    Res = [
        {<<"id">>, <<"s1f1">>},
        {<<"name">>, <<"Plik">>},
        {<<"type">>, <<"file">>},
        {<<"parent">>, <<"space1rootDir">>},
        {<<"children">>, []}
    ],
    {ok, Res};

find(<<"file">>, [<<"s1f2">>]) ->
    Res = [
        {<<"id">>, <<"s1f2">>},
        {<<"name">>, <<"File">>},
        {<<"type">>, <<"file">>},
        {<<"parent">>, <<"space1rootDir">>},
        {<<"children">>, []}
    ],
    {ok, Res};

find(<<"file">>, [<<"s2d1">>]) ->
    Res = [
        {<<"id">>, <<"s2d1">>},
        {<<"name">>, <<"Pusty I">>},
        {<<"type">>, <<"dir">>},
        {<<"parent">>, <<"space2rootDir">>},
        {<<"children">>, []}
    ],
    {ok, Res};

find(<<"file">>, [<<"s2d2">>]) ->
    Res = [
        {<<"id">>, <<"s2d2">>},
        {<<"name">>, <<"Pusty II">>},
        {<<"type">>, <<"dir">>},
        {<<"parent">>, <<"space2rootDir">>},
        {<<"children">>, []}
    ],
    {ok, Res};

find(<<"file">>, [<<"s2f1">>]) ->
    Res = [
        {<<"id">>, <<"s2d2">>},
        {<<"name">>, <<"Drugi Plik">>},
        {<<"type">>, <<"file">>},
        {<<"parent">>, <<"space2rootDir">>},
        {<<"children">>, []}
    ],
    {ok, Res}.

%% Called when ember asks for all files - not implemented, because we don't
%% want to return all files...
find_all(<<"file">>) ->
    {error, not_iplemented}.


%% Called when ember asks for file mathcing given query
find_query(<<"file">>, _Data) ->
    {error, not_iplemented}.

%% Called when ember asks to create a record
create_record(<<"file">>, _Data) ->
    {error, not_iplemented}.

%% Called when ember asks to update a record
update_record(<<"file">>, _Id, _Data) ->
    {error, not_iplemented}.

%% Called when ember asks to delete a record
delete_record(<<"file">>, _Id) ->
    {error, not_iplemented}.
