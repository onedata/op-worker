%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Tests datastore basic operations at all levels.
%%% It is utils module - it contains test functions but it is not
%%% test suite. These functions are included by suites that do tests
%%% using various environments.
%%% @end
%%%-------------------------------------------------------------------
-module(datastore_basic_ops_utils).
-author("Michal Wrzeszcz").

-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("annotations/include/annotations.hrl").


-export([create_delete_test/2, save_test/2, update_test/2, get_test/2, exists_test/2]).

%%%===================================================================
%%% Test function
%% ====================================================================


create_delete_test(Config, Level) ->
    ok.

save_test(Config, Level) ->
    ok.

update_test(Config, Level) ->
    ok.

get_test(Config, Level) ->
    ok.

exists_test(Config, Level) ->
    ok.




