%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2014 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This file contains ct tests helper macros and definitions.
%%% @end
%%%--------------------------------------------------------------------
-ifndef(TEST_UTILS_HRL).
-define(TEST_UTILS_HRL, 1).

-define(TEST, true).

-include_lib("dao/include/common.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/global_registry/gr_users.hrl").
-include_lib("ctool/include/global_registry/gr_spaces.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

%% Macro for cleaning test dirs before tests
-define(CLEAN_TEST_DIRS,begin
                          os:cmd("rm -rf /tmp/onedata/*"),
                          os:cmd("rm -rf /tmp/onedata2/*"),
                          os:cmd("rm -rf /tmp/onedata3/*"),
                          os:cmd("./clear_test_db.sh")
                        end).

%% oneprovider dependencies
-define(ONEPROVIDER_DEPS, [sasl,lager,ssl,cowlib,ranch,cowboy,ibrowse,gproc,meck]).

%% Returns absolute path to given file using virtual CWD which equals to current SUITE directory
-define(TEST_FILE(X), filename:join(ets:match(suite_state, {test_root, '$1'}) ++ [X])).

%% Returns absolute path to given file using virtual CWD which equals to ct_root/common_files
-define(COMMON_FILE(X), filename:join(ets:match(suite_state, {ct_root, '$1'}) ++ ["common_files"] ++ [X])).

%% Roots of test filesystem
-define(TEST_ROOT, "/tmp/onedata/").
-define(TEST_ROOT2, "/tmp/onedata2/").
-define(TEST_ROOT3, "/tmp/onedata3/").

-define(ARG_TEST_ROOT, [?TEST_ROOT]).
-define(ARG_TEST_ROOT2, [?TEST_ROOT2]).
-define(ARG_TEST_ROOT3, [?TEST_ROOT3]).

% Test users and groups
-define(TEST_USER, "onedatatestuser").
-define(TEST_USER2, "onedatatestuser2").
-define(TEST_GROUP, "onedatatestgroup").
-define(TEST_GROUP2, "onedatatestgroup2").
-define(TEST_GROUP3, "onedatatestgroup3").
-define(TEST_GROUP_EXTENDED, "onedatatestgroup(Grp)").
-define(TEST_GROUP2_EXTENDED, "onedatatestgroup2(Grp2)").
-define(TEST_GROUP3_EXTENDED, "onedatatestgroup3(Grp3)").

-endif.