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


-export([
    set_user_privileges/3,
    format_record/1
]).


%%%===================================================================
%%% API
%%%===================================================================


-spec set_user_privileges(
    oct_background:entity_selector(),
    oct_background:entity_selector(),
    [privileges:space_privilege()]
) ->
    ok.
set_user_privileges(UserSelector, SpaceSelector, Privileges) ->
    UserId = oct_background:get_user_id(UserSelector),
    SpaceId = oct_background:get_space_id(SpaceSelector),
    ozw_test_rpc:space_set_user_privileges(SpaceId, UserId, Privileges).


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

get_record_def(dataset_obj, N) ->
    case record_info(size, dataset_obj) - 1 of
        N -> record_info(fields, dataset_obj);
        _ -> no
    end;

get_record_def(_, _) ->
    no.
