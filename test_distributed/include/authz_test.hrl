%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019-2024 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% This file contains definitions of macros used in authz tests.
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


-record(ct_authz_file_spec, {
    % name of file
    name :: binary(),
    % permissions needed to perform #test_spec.operation
    perms = [] :: [Perms :: binary()],
    % function called during environment setup. Term returned will be stored in `ExtraData`
    % and can be used during test (described in `operation` of #authz_test_suite_spec{}).
    on_create = undefined :: undefined | fun((node(), session:id(), file_id:file_guid()) -> term())
}).

-record(ct_authz_dir_spec, {
    % name of directory
    name :: binary(),
    % permissions needed to perform #test_spec.operation
    perms = [] :: [Perms :: binary()],
    % function called during environment setup. Term returned will be stored in `ExtraData`
    % and can be used during test (described in `operation` of #authz_test_suite_spec{}).
    on_create = undefined :: undefined | fun((session:id(), file_id:file_guid()) -> term()),
    % children of directory if needed
    children = [] :: [#ct_authz_dir_spec{} | #ct_authz_file_spec{}]
}).

-record(authz_test_suite_spec, {
    % Unique name of test suite.
    name :: binary(),

    % Selector of provider on which tests will be carried.
    provider_selector = krakow :: oct_background:entity_selector(),

    % Selector of space within which tests will be carried.
    space_selector = space_krk :: oct_background:entity_selector(),

    % Selector of user being owner of space. He should be allowed to perform
    % any operation on files in space regardless of permissions set.
    space_owner_selector = user1 :: oct_background:entity_selector(),

    % Selector of user belonging to space specified in `space_selector` in
    % context of which all files required for tests will be created. It will
    % be used to test `user` posix bits and `OWNER@` special acl identifier.
    files_owner_selector = user2 :: oct_background:entity_selector(),

    % Selector of user belonging to space specified in `space_selector` which
    % aren't the same as `owner_user`. It will be used to test `group` posix
    % bits and acl for his Id.
    space_user_selector = user3 :: oct_background:entity_selector(),

    % Selector of group to which belongs `space_user_selector` and which itself
    % belong to `space_selector`. It will be used to test acl group identifier.
    space_user_group_selector = group2 :: oct_background:entity_selector(),

    % Selector of user not belonging to space specified in `space_selector`.
    % It will be used to test `other` posix bits and `EVERYONE@` special acl
    % identifier.
    non_space_user_selector = user4 :: oct_background:entity_selector(),

    % Tells whether `operation` needs `traverse_ancestors` permission. If so
    % `traverse_container` perm will be added to test root dir as needed perm
    % to perform `operation` (since traverse_ancestors means that one can
    % traverse dirs up to file in question).
    requires_traverse_ancestors = true :: boolean(),

    % Tells which space privileges are needed to perform `operation`
    % in case of posix access mode.
    posix_requires_space_privs = [] ::
        % only owner with specified privs can perform operation
        {file_owner, [privileges:space_privilege()]} |
        % any user with specified privs can perform
        [privileges:space_privilege()],

    % Tells which space privileges are needed to perform `operation`
    % in case of acl access mode
    acl_requires_space_privs = [] :: [privileges:space_privilege()],

    % Description of environment (files and permissions on them) needed to
    % perform `operation`.
    files :: [#ct_authz_dir_spec{} | #ct_authz_file_spec{}],

    % Tells whether operation should work in readonly mode (readonly caveats set)
    available_in_readonly_mode = false :: boolean(),

    % Tells whether operation should work in share mode. For some operation this
    % check is entirely inapplicable due to operation call not using file guid
    % (can't be called via shared guid == no share mode).
    available_in_share_mode = false :: boolean() | inapplicable,

    % Tells whether operation should work in open handle mode.
    available_in_open_handle_mode = false :: boolean(),

    % Operation being tested. It will be called for various combinations of
    % either posix or acl permissions. It is expected to fail for combinations
    % not having all perms specified in `files` and space privileges and
    % succeed for combination consisting of only them.
    % It takes following arguments:
    % - TestNode - node on which operation should be performed,
    % - ExecutionerSessId - session id of user which should perform operation,
    % - TestCaseRootDirPath - absolute path to root dir of testcase,
    % - ExtraData - mapping of file path (for every file specified in `files`) to
    %               term returned from `on_create` #ct_authz_dir_spec{} or #ct_authz_file_spec{} fun.
    %               If mentioned fun is left undefined then by default ?FILE_REF(GUID) will
    %               be used.
    %               If `on_create` fun returns FileGuid it should be returned as
    %               following tuple ?FILE_REF(FileGuid), which is required by framework.
    operation :: fun((node(), session:id(), file_meta:path(), map()) -> ok | {error, term()}),

    % Tells whether failed operation returns:
    % - old 'errno_errors' in format {error, Errno} (e.g. {error, enoent}) - see errno.hrl
    % - new 'api_errors' defined in errors.hrl
    returned_errors = errno_errors :: errno_errors | api_errors,

    % Tells whether successfully executed operation should change ownership on underlying storage
    final_ownership_check = fun(_) -> skip end :: fun((TestCaseRootDirPath :: file_meta:path()) ->
        skip |
        {should_preserve_ownership, LogicalFilePath :: file_meta:path()} |
        {should_change_ownership, LogicalFilePath :: file_meta:path()}
    )
}).

-endif.
