%%%-------------------------------------------------------------------
%%% @author Michal Wrona
%%% @copyright (C) 2016-2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests of the bookkeeping the provider does of the handles held to a file -
%%% how many descriptors each session has open on it, and hence whether the file
%%% is still in use. The point of it all is the moment the last descriptor goes
%%% away: if the file was deleted in the meantime, that is when it is finally
%%% handed over to be removed, which is what the rest of this suite tests in
%%% terms of what it leaves on the storage.
%%%
%%% Unlike every other case of the suite, these work on the file_handles model
%%% directly, which knows files by their id alone - a made-up id belonging to no
%%% space that exists is therefore enough, and no space, storage or file is set
%%% up for them. They are kept here, rather than in a suite of their own, exactly
%%% because they cost the deployment nothing: they cover the mechanism the
%%% deletion cases depend on, one level below where those observe it.
%%% @end
%%%-------------------------------------------------------------------
-module(file_handles_tests).
-author("Michal Wrona").

% This module indirectly includes eunit.hrl, whose parse transform would
% otherwise auto-export every arity 0 function named *_test - clashing with
% the export list below.
-define(EUNIT_NOAUTO, 1).

-include("file/file_lifecycle_test.hrl").
-include("modules/fslogic/fslogic_delete.hrl").
-include_lib("ctool/include/test/assertions.hrl").

%% tests
-export([
    counting_file_open_and_release_test/1,
    session_deletion_releases_its_open_files_test/1
]).
%% API
-export([clean_up/1]).

% The file the cases below keep their descriptors on. It exists nowhere but in
% the file_handles model, which is all these cases are about; the ids are given
% a prefix of their own because the deployment is shared and a session id is
% derived from the nonce alone.
-define(FILE_UUID, <<"file_handles_test_file">>).
-define(FILE_GUID, file_id:pack_guid(?FILE_UUID, <<"file_handles_test_space">>)).

-define(NONCE_1, <<"file_handles_test_nonce_1">>).
-define(NONCE_2, <<"file_handles_test_nonce_2">>).

% Sent by the mock below in place of the file deletion it stands in for - there
% is no file to delete, so being called at all with the right file is the whole
% of what the cases assert.
-define(HANDED_OVER_FOR_DELETION(__FILE_UUID, __REMOVAL_STATUS),
    {handed_over_for_deletion, __FILE_UUID, __REMOVAL_STATUS}
).


%%%====================================================================
%%% Test functions
%%%====================================================================


counting_file_open_and_release_test(_Config) ->
    Node = file_lifecycle_test_utils:get_node(),
    FileCtx = file_ctx:new_by_guid(?FILE_GUID),
    SessId = create_session(Node, ?NONCE_1),
    mock_handing_over_for_deletion(Node),

    ?assertEqual(false, is_file_opened(Node)),

    % a file counts as opened as long as any of the descriptors registered for it
    % is still held, no matter how many were registered at a time
    ?assertEqual(ok, register_open(Node, FileCtx, SessId, 30)),
    ?assertEqual(true, is_file_opened(Node)),
    ?assertEqual(ok, register_open(Node, FileCtx, SessId, 70)),
    ?assertEqual(true, is_file_opened(Node)),

    ?assertEqual(ok, register_release(Node, FileCtx, SessId, 50)),
    ?assertEqual(true, is_file_opened(Node)),
    ?assertEqual(ok, register_release(Node, FileCtx, SessId, 30)),
    ?assertEqual(true, is_file_opened(Node)),
    ?assertEqual(ok, register_release(Node, FileCtx, SessId, 20)),
    ?assertEqual(false, is_file_opened(Node)),

    % releasing a file that holds no descriptors any more must not fail
    ?assertEqual(ok, register_release(Node, FileCtx, SessId, 50)),
    ?assertEqual(false, is_file_opened(Node)),

    % the release of the last descriptor of a file marked for removal is what
    % hands the file over to be deleted
    ?assertEqual(ok, register_open(Node, FileCtx, SessId, 1)),
    ?assertEqual(ok, mark_to_remove(Node, FileCtx)),
    ?assertEqual(ok, register_release(Node, FileCtx, SessId, 1)),
    ?assertReceivedEqual(?HANDED_OVER_FOR_DELETION(?FILE_UUID, ?LOCAL_REMOVE), ?TIMEOUT).


session_deletion_releases_its_open_files_test(_Config) ->
    Node = file_lifecycle_test_utils:get_node(),
    FileCtx = file_ctx:new_by_guid(?FILE_GUID),
    mock_handing_over_for_deletion(Node),

    % the descriptors a session held are released along with the session itself
    SessId1 = create_session(Node, ?NONCE_1),
    ?assertEqual(ok, register_open(Node, FileCtx, SessId1, 30)),
    ?assertEqual(true, is_file_opened(Node)),
    ?assertEqual(ok, delete_session(Node, SessId1)),
    ?assertEqual(false, is_file_opened(Node)),

    % with several sessions holding descriptors on the same file, it stops being
    % opened only once the last of them is gone. NOTE: a session id is derived
    % from its nonce, so reconnecting with the one used before - as a client that
    % lost its connection would - brings back a session of the very same id.
    ?assertEqual(SessId1, create_session(Node, ?NONCE_1)),
    SessId2 = create_session(Node, ?NONCE_2),
    ?assertEqual(ok, register_open(Node, FileCtx, SessId1, 30)),
    ?assertEqual(true, is_file_opened(Node)),
    ?assertEqual(ok, register_open(Node, FileCtx, SessId2, 30)),
    ?assertEqual(true, is_file_opened(Node)),
    ?assertEqual(ok, delete_session(Node, SessId1)),
    ?assertEqual(true, is_file_opened(Node)),
    ?assertEqual(ok, delete_session(Node, SessId2)),
    ?assertEqual(false, is_file_opened(Node)),

    % and if the file was marked for removal in the meantime, the last session to
    % go is what hands it over to be deleted
    ?assertEqual(SessId1, create_session(Node, ?NONCE_1)),
    ?assertEqual(ok, register_open(Node, FileCtx, SessId1, 30)),
    ?assertEqual(ok, mark_to_remove(Node, FileCtx)),
    ?assertEqual(ok, delete_session(Node, SessId1)),
    ?assertReceivedEqual(?HANDED_OVER_FOR_DELETION(?FILE_UUID, ?LOCAL_REMOVE), ?TIMEOUT),

    % invalidating an entry of a file that has none left, and of a session that
    % is gone along with it, must not fail
    ?assertEqual(ok, invalidate_session_entry(Node, FileCtx, SessId1)),

    % nor must invalidating the entry of a session that does hold descriptors -
    % that is the ordinary way they go away
    ?assertEqual(SessId1, create_session(Node, ?NONCE_1)),
    ?assertEqual(ok, register_open(Node, FileCtx, SessId1, 30)),
    ?assertEqual(ok, invalidate_session_entry(Node, FileCtx, SessId1)),
    ?assertEqual(false, is_file_opened(Node)).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Undoes what the cases of this module leave on the provider. NOTE: this must
%% run even after a case that failed halfway through - a file_handles document
%% left marked for removal makes every later register_open on it fail, and the
%% sessions outlive the run, as the suite holds off their garbage collection.
%% @end
%%--------------------------------------------------------------------
-spec clean_up(node()) -> ok.
clean_up(Node) ->
    lists:foreach(fun(Nonce) ->
        delete_session(Node, build_session_id(Nonce))
    end, [?NONCE_1, ?NONCE_2]),
    rpc:call(Node, file_handles, delete, [?FILE_UUID]),
    ok.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
%% @doc
%% Stands in for the deletion of the file whose last descriptor was just
%% released - the file of these cases exists nowhere but in the file_handles
%% model, so there is nothing to actually delete. Any other file released on the
%% provider at that moment must be left to take its usual course.
%% @end
-spec mock_handing_over_for_deletion(node()) -> ok.
mock_handing_over_for_deletion(Node) ->
    Master = self(),

    file_lifecycle_test_utils:mock(Node, fslogic_delete, handle_release_of_deleted_file,
        fun(FileCtx, RemovalStatus) ->
            case file_ctx:get_logical_uuid_const(FileCtx) of
                ?FILE_UUID ->
                    Master ! ?HANDED_OVER_FOR_DELETION(?FILE_UUID, RemovalStatus),
                    ok;
                _ ->
                    meck:passthrough([FileCtx, RemovalStatus])
            end
        end
    ).


%% @private
-spec create_session(node(), binary()) -> session:id().
create_session(Node, Nonce) ->
    {ok, SessId} = fuse_test_utils:setup_fuse_session(
        Node,
        oct_background:get_user_id(?USER_SELECTOR),
        Nonce,
        oct_background:get_user_access_token(?USER_SELECTOR)
    ),
    SessId.


%% @private
-spec build_session_id(binary()) -> session:id().
build_session_id(Nonce) ->
    datastore_key:new_from_digest([<<"fuse">>, Nonce]).


%% @private
-spec delete_session(node(), session:id()) -> ok | {error, term()}.
delete_session(Node, SessId) ->
    rpc:call(Node, session, delete, [SessId]).


%% @private
-spec is_file_opened(node()) -> boolean().
is_file_opened(Node) ->
    rpc:call(Node, file_handles, is_file_opened, [?FILE_UUID]).


%% @private
-spec register_open(node(), file_ctx:ctx(), session:id(), pos_integer()) ->
    ok | {error, term()}.
register_open(Node, FileCtx, SessId, Count) ->
    rpc:call(Node, file_handles, register_open, [FileCtx, SessId, Count, undefined]).


%% @private
-spec register_release(node(), file_ctx:ctx(), session:id(), pos_integer()) ->
    ok | {error, term()}.
register_release(Node, FileCtx, SessId, Count) ->
    rpc:call(Node, file_handles, register_release, [FileCtx, SessId, Count]).


%% @private
-spec mark_to_remove(node(), file_ctx:ctx()) -> ok | {error, term()}.
mark_to_remove(Node, FileCtx) ->
    rpc:call(Node, file_handles, mark_to_remove, [FileCtx, ?LOCAL_REMOVE]).


%% @private
-spec invalidate_session_entry(node(), file_ctx:ctx(), session:id()) -> ok | {error, term()}.
invalidate_session_entry(Node, FileCtx, SessId) ->
    rpc:call(Node, file_handles, invalidate_session_entry, [FileCtx, SessId]).
