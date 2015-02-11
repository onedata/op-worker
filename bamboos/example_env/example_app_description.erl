%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This is an example app description used by appmock.
%%%
%%% This file is referenced from gen_dev_args.json and
%%% must be in the same directory for appmock_up.py to work.
%%% @end
%%%-------------------------------------------------------------------
-module(example_app_description).
-behaviour(mock_app_description_behaviour).

-include("appmock.hrl").
-include_lib("ctool/include/logging.hrl").

-export([response_mocks/0]).

response_mocks() -> [
    #mock_resp_mapping{port = 8080, path = <<"/test1">>, response = #mock_resp{body = <<"this is test1 endpoint">>}},

    #mock_resp_mapping{port = 8080, path = <<"/test2">>, response = #mock_resp{body = <<"this is test2 endpoint">>}},

    #mock_resp_mapping{port = 9090, path = <<"/test_with_state">>,
        response = fun(_Req, State) ->
            R = #mock_resp{body = <<"Counter: ", (integer_to_binary(State))/binary>>},
            {R, State + 1}
        end,
    initial_state = 0}
].