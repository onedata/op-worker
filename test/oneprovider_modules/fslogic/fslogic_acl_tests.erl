%% ===================================================================
%% @author Tomasz Lichon
%% @copyright (C): 2014, ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc This module tests the functionality of fslogic_acl.
%% It contains unit tests that base on eunit.
%% @end
%% ===================================================================
-module(fslogic_acl_tests).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-include("oneprovider_modules/fslogic/fslogic_acl.hrl").
-include("fuse_messages_pb.hrl").
-include("oneprovider_modules/fslogic/fslogic.hrl").

check_permission_test() ->
    Id1 = <<"id1">>,
    Ace1 = #accesscontrolentity{acetype = ?allow_mask, aceflags = ?no_flags_mask, identifier = Id1, acemask = ?read_mask},
    Ace2 = #accesscontrolentity{acetype = ?allow_mask, aceflags = ?no_flags_mask, identifier = Id1, acemask = ?write_mask},
    % read permission
    ?assertEqual(ok, fslogic_acl:check_permission([Ace1, Ace2], Id1, ?read_mask)),
    % rdwr permission on different ACEs
    ?assertEqual(ok, fslogic_acl:check_permission([Ace1, Ace2], Id1, ?read_mask bor ?write_mask)),
    % rwx permission, not allowed
    ?assertEqual(?VEPERM, catch fslogic_acl:check_permission([Ace1, Ace2], Id1, ?read_mask bor ?write_mask bor ?execute_mask)),
    % x permission, not allowed
    ?assertEqual(?VEPERM, catch fslogic_acl:check_permission([Ace1, Ace2], Id1, ?execute_mask)),

    Id2 = <<"id2">>,
    Ace3 = #accesscontrolentity{acetype = ?deny_mask, aceflags = ?no_flags_mask, identifier = Id2, acemask = ?read_mask},
    Ace4 = #accesscontrolentity{acetype = ?deny_mask, aceflags = ?no_flags_mask, identifier = Id1, acemask = ?read_mask},
    % read permission, with denying someone's else read
    ?assertEqual(ok, fslogic_acl:check_permission([Ace3, Ace1, Ace2], Id1, ?read_mask)),
    % read permission, with denying read
    ?assertEqual(?VEPERM, catch fslogic_acl:check_permission([Ace2, Ace4, Ace1], Id2, ?read_mask bor ?write_mask)).



-endif.