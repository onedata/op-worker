%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% This file contains the dictionary of file permission definitions shared by
%%% all authorization tests. Test suite specs (which differ between test
%%% frameworks) are defined alongside their frameworks.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(AUTHZ_TEST_HRL).
-define(AUTHZ_TEST_HRL, 1).

-include("modules/fslogic/acl.hrl").
-include("modules/fslogic/data_access_control.hrl").

-define(ALL_PERMS, [
    ?read_object,
    ?list_container,
    ?write_object,
    ?add_object,
    ?add_subcontainer,
    ?read_metadata,
    ?write_metadata,
    ?traverse_container,
    ?delete_object,
    ?delete_subcontainer,
    ?read_attributes,
    ?write_attributes,
    ?delete,
    ?read_acl,
    ?write_acl
]).
-define(DIR_SPECIFIC_PERMS, [
    ?list_container,
    ?add_object,
    ?add_subcontainer,
    ?traverse_container,
    ?delete_object,
    ?delete_subcontainer
]).
-define(FILE_SPECIFIC_PERMS, [
    ?read_object,
    ?write_object
]).
-define(ALL_FILE_PERMS, (?ALL_PERMS -- ?DIR_SPECIFIC_PERMS)).
-define(ALL_DIR_PERMS, (?ALL_PERMS -- ?FILE_SPECIFIC_PERMS)).

-define(ALL_POSIX_PERMS, [read, write, exec]).


-define(ALLOW_ACE(__IDENTIFIER, __FLAGS, __MASK), #access_control_entity{
    acetype = ?allow_mask,
    identifier = __IDENTIFIER,
    aceflags = __FLAGS,
    acemask = __MASK
}).

-define(DENY_ACE(__IDENTIFIER, __FLAGS, __MASK), #access_control_entity{
    acetype = ?deny_mask,
    aceflags = __FLAGS,
    identifier = __IDENTIFIER,
    acemask = __MASK
}).

-endif.
