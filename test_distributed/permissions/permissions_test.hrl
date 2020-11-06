%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% This file contains definitions of macros used in lfm_permissions tests.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(LFM_PERMISSIONS_TEST_HRL).
-define(LFM_PERMISSIONS_TEST_HRL, 1).

-include_lib("ctool/include/posix/acl.hrl").

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


-record(file, {
    % name of file
    name :: binary(),
    % permissions needed to perform #test_spec.operation
    perms = [] :: [Perms :: binary()],
    % function called during environment setup. Term returned will be stored in `ExtraData`
    % and can be used during test (described in `operation` of #test_spec{}).
    on_create = undefined :: undefined | fun((OwnerSessId :: session:id(), file_id:file_guid()) -> term())
}).

-record(dir, {
    % name of directory
    name :: binary(),
    % permissions needed to perform #test_spec.operation
    perms = [] :: [Perms :: binary()],
    % function called during environment setup. Term returned will be stored in `ExtraData`
    % and can be used during test (described in `operation` of #test_spec{}).
    on_create = undefined :: undefined | fun((session:id(), file_id:file_guid()) -> term()),
    % children of directory if needed
    children = [] :: [#dir{} | #file{}]
}).

-record(perms_test_spec, {
    test_node :: node(),

    % Id of space within which test will be carried
    space_id = <<"space1">> :: binary(),

    % Name of root dir for test
    root_dir :: binary(),

    % Id of user being owner of space. He should be allowed to perform any
    % operation on files in space regardless of permissions set.
    space_owner = <<"owner">> :: binary(),

    % Id of user belonging to space specified in `space_id` in context
    % of which all files required for tests will be created. It will be
    % used to test `user` posix bits and `OWNER@` special acl identifier
    owner_user = <<"user1">> :: binary(),

    % Id of user belonging to space specified in `space_id` which aren't
    % the same as `owner_user`. It will be used to test `group` posix bits
    % and acl for his Id.
    space_user = <<"user2">> :: binary(),

    % Id of group to which belongs `space_user` and which itself belong to
    % `space_id`. It will be used to test acl group identifier.
    space_user_group = <<"group2">> :: binary(),

    % Id of user not belonging to space specified in `space_id`. It will be
    % used to test `other` posix bits and `EVERYONE@` special acl identifier.
    other_user = <<"user3">> :: binary(),

    % Tells whether `operation` needs `traverse_ancestors` permission. If so
    % `traverse_container` perm will be added to test root dir as needed perm
    % to perform `operation` (since traverse_ancestors means that one can
    % traverse dirs up to file in question).
    requires_traverse_ancestors = true :: boolean(),

    % Tells which space privileges are needed to perform `operation`
    % in case of posix access mode
    posix_requires_space_privs = [] :: owner | [privileges:space_privilege()],

    % Tells which space privileges are needed to perform `operation`
    % in case of acl access mode
    acl_requires_space_privs = [] :: owner | [privileges:space_privilege()],

    % Description of environment (files and permissions on them) needed to
    % perform `operation`.
    files :: [#dir{} | #file{}],

    % Tells whether operation should work in readonly mode (readonly caveats set)
    available_in_readonly_mode = false :: boolean(),

    % Tells whether operation should work in share mode. For some operation this
    % check is entirely inapplicable due to operation call not using file guid
    % (can't be called via shared guid == no share mode).
    available_in_share_mode = false :: boolean() | inapplicable,

    % Operation being tested. It will be called for various combinations of
    % either posix or acl permissions. It is expected to fail for combinations
    % not having all perms specified in `files` and space privileges and
    % succeed for combination consisting of only them.
    % It takes following arguments:
    % - OwnerSessId - session id of user which creates files for this test,
    % - SessId - session id of user which should perform operation,
    % - TestCaseRootDirPath - absolute path to root dir of testcase,
    % - ShareId - Id only in case of share tests. Otherwise left as `undefined`,
    % - ExtraData - mapping of file path (for every file specified in `files`) to
    %               term returned from `on_create` #dir{} or #file{} fun.
    %               If mentioned fun is left undefined then by default {guid, GUID} will
    %               be used.
    %               If `on_create` fun returns FileGuid it should be returned as
    %               following tuple {guid, FileGuid}, which is required by framework.
    operation :: fun((OwnerSessId :: binary(), SessId :: binary(), TestCaseRootDirPath :: binary(), ExtraData :: map()) ->
        ok |
        {ok, term()} |
        {ok, term(), term()} |
        {ok, term(), term(), term()} |
        {error, term()}
    )
}).

-endif.
