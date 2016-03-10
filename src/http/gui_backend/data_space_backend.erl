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
-module(data_space_backend).
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
    ?alert("~s", [str_utils:format("DATA_SPACE_BACKEND: ~s: ~p", [??_Arg, _Arg])])
).

init() ->
    ?log_debug({websocket_init, g_session:get_session_id()}),
    ok.

%% Called when ember asks for a particular dataSpace
find(<<"dataSpace">>, [<<"space1">>]) ->
    Res = [
        {<<"id">>, <<"space1">>},
        {<<"name">>, <<"Space 1">>},
        {<<"isDefault">>, false},
        {<<"rootDir">>, <<"space1rootDir">>}
    ],
    ?log_debug({find, Res}),
    {ok, Res};

find(<<"dataSpace">>, [<<"space2">>]) ->
    Res = [
        {<<"id">>, <<"space2">>},
        {<<"name">>, <<"Space 2">>},
        {<<"isDefault">>, true},
        {<<"rootDir">>, <<"space2rootDir">>}
    ],
    ?log_debug({find, Res}),
    {ok, Res}.

%% Called when ember asks for all files - not implemented, because we don't
%% want to return all files...
find_all(<<"dataSpace">>) ->
    {ok, DS1} = find(<<"dataSpace">>, [<<"space1">>]),
    {ok, DS2} = find(<<"dataSpace">>, [<<"space2">>]),
    {ok, [DS1, DS2]}.


%% Called when ember asks for file mathcing given query
find_query(<<"dataSpace">>, _Data) ->
    {error, not_iplemented}.

%% Called when ember asks to create a record
create_record(<<"dataSpace">>, _Data) ->
    {error, not_iplemented}.

update_record(<<"dataSpace">>, _Id, _Data) ->
    {error, not_iplemented}.

%% Called when ember asks to delete a record
delete_record(<<"dataSpace">>, _Id) ->
    {error, not_iplemented}.
