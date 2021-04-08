%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% General utility functions used in onenv ct tests.
%%% @end
%%%-------------------------------------------------------------------
-module(onenv_test_utils).
-author("Bartosz Walkowicz").

-include("onenv_test_utils.hrl").
-include("distribution_assert.hrl").
-include_lib("ctool/include/test/test_utils.hrl").


-export([format_record/1]).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Returns string with nicely formatted record (with field names, etc.).
%% Useful for debug.
%% @end
%%--------------------------------------------------------------------
-spec format_record(term()) -> io_lib:chars().
format_record(Record) ->
    io_lib_pretty:print(Record, fun get_record_def/2).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec get_record_def(atom(), integer()) -> no | [FieldName :: atom()].
get_record_def(object, N) ->
    case record_info(size, object) - 1 of
        N -> record_info(fields, object);
        _ -> no
    end;

get_record_def(dataset_object, N) ->
    case record_info(size, dataset_object) - 1 of
        N -> record_info(fields, dataset_object);
        _ -> no
    end;

get_record_def(_, _) ->
    no.
