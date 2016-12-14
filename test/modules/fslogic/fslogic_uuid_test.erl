%%%--------------------------------------------------------------------
%%% @author Konrad Zemek
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Tests for fslogic_uuid fslogic model.
%%% @end
%%%--------------------------------------------------------------------
-module(fslogic_uuid_test).
-author("Konrad Zemek").

-include("modules/datastore/datastore_specific_models_def.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("eunit/include/eunit.hrl").

parent_uuid_test_() ->
    {foreach,
        fun start/0,
        fun stop/1,
        [
            fun parent_uuid_should_return_parent_uuid/1,
            fun parent_uuid_should_return_undefined_for_user_root_dir/1,
            fun parent_uuid_should_return_undefined_for_root_dir/1,
            fun parent_uuid_should_return_user_root_dir_for_user_toplevel_children/1,
            fun parent_uuid_should_return_user_root_dir_for_toplevel_children/1
        ]}.

parent_uuid_should_return_parent_uuid(UserId) ->
    [?_assertEqual(<<"ParentDirUuid">>,
        fslogic_uuid:parent_uuid({child_of, <<"ParentDirUuid">>}, UserId))].

parent_uuid_should_return_undefined_for_user_root_dir(UserId) ->
    UserRootUuid = fslogic_uuid:user_root_dir_uuid(UserId),
    [?_assertEqual(undefined,
        fslogic_uuid:parent_uuid(UserRootUuid, UserId))].

parent_uuid_should_return_undefined_for_root_dir(UserId) ->
    [?_assertEqual(undefined,
        fslogic_uuid:parent_uuid(?ROOT_DIR_UUID, UserId))].

parent_uuid_should_return_user_root_dir_for_user_toplevel_children(UserId) ->
    UserRootUuid = fslogic_uuid:user_root_dir_uuid(UserId),
    [?_assertEqual(UserRootUuid,
        fslogic_uuid:parent_uuid({child_of, UserRootUuid}, UserId))].

parent_uuid_should_return_user_root_dir_for_toplevel_children(UserId) ->
    UserRootUuid = fslogic_uuid:user_root_dir_uuid(UserId),
    [?_assertEqual(UserRootUuid,
        fslogic_uuid:parent_uuid({child_of, ?ROOT_DIR_UUID}, UserId))].

start() ->
    meck:new(file_meta),

    meck:expect(file_meta, get,
        fun(ChildFileUuid) -> {ok, #document{key = ChildFileUuid}} end),

    meck:expect(file_meta, get_parent_uuid,
        fun(#document{key = {child_of, Uuid}}) -> {ok, Uuid} end),

    <<"UserId">>.

stop(_) ->
    meck:unload(file_meta).
