%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Macros and test data used in file registration tests (POST data/register).
%%% @end
%%%-------------------------------------------------------------------
-ifndef(FILE_REGISTRATION_TEST_HRL).
-define(FILE_REGISTRATION_TEST_HRL, 1).


-include("space_setup_utils.hrl").
-include("modules/fslogic/fslogic_suffix.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/posix/errno.hrl").
-include_lib("ctool/include/http/codes.hrl").
-include_lib("ctool/include/http/headers.hrl").


%% Identifies a concrete instantiation of the (shared) file registration test
%% logic - which storage type backs the registering provider and which
%% providers/user take part. Filled in by the thin SUITE modules.
-record(file_registration_test_suite_ctx, {
    registering_storage_type :: s3 | http,
    registering_provider_selector :: oct_background:entity_selector(),
    other_provider_selector :: oct_background:entity_selector(),
    test_user_selector :: oct_background:entity_selector()
}).


-define(ATTEMPTS, 30).

%% Names
-define(FILE_NAME, <<"file_", (?RAND_STR())/binary>>).
-define(DIR_NAME, <<"dir_", (?RAND_STR())/binary>>).

%% File content
-define(TEST_DATA, <<"abcdefgh">>).
-define(TEST_DATA2, <<"zyxwvut">>).

%% Extended attributes
-define(XATTR_KEY(N), <<"xattrName", (integer_to_binary(N))/binary>>).
-define(XATTR_VALUE(N), <<"xattrValue", (integer_to_binary(N))/binary>>).
-define(XATTRS, #{
    ?XATTR_KEY(1) => 1,
    ?XATTR_KEY(2) => ?XATTR_VALUE(2)
}).
-define(XATTRS2, #{
    ?XATTR_KEY(1) => ?XATTR_VALUE(1),
    ?XATTR_KEY(3) => ?XATTR_VALUE(3)
}).

%% JSON metadata
-define(JSON1, #{
    <<"key1">> => <<"value1">>,
    <<"key2">> => #{
        <<"key21">> => <<"value21">>
    }
}).
-define(JSON2, #{
    <<"key1">> => <<"value1.2">>,
    <<"key3">> => #{
        <<"key31">> => <<"value31">>
    }
}).

%% RDF metadata
-define(RDF1, <<"<rdf>metadata_1</rdf>">>).
-define(RDF2, <<"<rdf>metadata_2</rdf>">>).
-define(ENCODED_RDF(RDF), base64:encode(RDF)).
-define(ENCODED_RDF1, ?ENCODED_RDF(?RDF1)).
-define(ENCODED_RDF2, ?ENCODED_RDF(?RDF2)).


%%%===================================================================
%%% Assertion macros
%%%===================================================================

-define(assertInLs(Worker, SessId, FilePath, Attempts), (
    fun(__Worker, __SessId, __FilePath, __Attempts) ->
        ?assertMatch(true, try
            __DirPath = filename:dirname(__FilePath),
            {ok, __Children} = lfm_proxy:get_children(__Worker, __SessId, {path, __DirPath}, 0, 10000),
            __ChildrenNames = [_N || {_G, _N} <- __Children],
            lists:member(filename:basename(__FilePath), __ChildrenNames)
        catch
            _:_ ->
                error
        end, __Attempts)
    end)(Worker, SessId, FilePath, Attempts)
).

-define(assertStat(Worker, SessId, FilePath, Attempts), (
    fun(__Worker, __SessId, __FilePath, __Attempts) ->
        ExpName__Local = filename:basename(__FilePath),

        ?assertMatch(
            {ok, #file_attr{name = ExpName__Local}},
            lfm_proxy:stat(__Worker, __SessId, {path, __FilePath}),
            __Attempts
        )
    end)(Worker, SessId, FilePath, Attempts)
).

%% The file is (re)opened on every attempt. A handle opened on a provider before
%% the file_location carrying the source provider's data blocks has synced caches
%% an empty location in its file_ctx, so the requested range reads back as zeros
%% (a hole) and retrying the read on that same handle never recovers. Reopening on
%% each attempt picks up the synced location - see the analogous read-verification
%% loop in multi_provider_file_ops_test_base.
-define(assertRead(Worker, SessId, FilePath, Offset, ExpectedData, Attempts), (
    fun(__Worker, __SessId, __FilePath, __Offset, __ExpectedData, __Attempts) ->
        ?assertEqual({ok, __ExpectedData}, begin
            {ok, __H} = lfm_proxy:open(__Worker, __SessId, {path, __FilePath}, read),
            try
                lfm_proxy:read(__Worker, __H, __Offset, byte_size(__ExpectedData))
            after
                lfm_proxy:close(__Worker, __H)
            end
        end, __Attempts)
    end)(Worker, SessId, FilePath, Offset, ExpectedData, Attempts)
).

-define(assertXattrs(Worker, SessId, FilePath, Xattrs, Attempts),
    (fun(__Worker, __SessId, __FilePath, __Xattrs, __Attempts) ->
        ?assertEqual(#{}, maps:fold(fun(ExpName__Local, ExpValue__Local, __Acc) ->
            ?assertMatch(
                {ok, #xattr{name = ExpName__Local, value = ExpValue__Local}},
                lfm_proxy:get_xattr(__Worker, __SessId, {path, __FilePath}, ExpName__Local),
                __Attempts
            ),
            maps:without([ExpName__Local], __Acc)
        end, __Xattrs, __Xattrs), __Attempts)
    end)(Worker, SessId, FilePath, Xattrs, Attempts)
).

-define(resolveFileRef(Worker, SessId, FilePath),
    ?FILE_REF(element(2, {ok, _} = lfm_proxy:resolve_guid(Worker, SessId, FilePath)))
).

-define(assertJsonMetadata(Worker, SessId, FilePath, JSON, Attempts),
    (fun
        (__Worker, __SessId, __FilePath, ExpJson__Local, __Attempts) when map_size(ExpJson__Local) =:= 0 ->
            FileRef = ?resolveFileRef(__Worker, __SessId, __FilePath),

            ?assertMatch(
                ?ERR_POSIX(?ENODATA),
                opt_file_metadata:get_custom_metadata(__Worker, __SessId, FileRef, json, [], false),
                __Attempts
            );
        (__Worker, __SessId, __FilePath, ExpJson__Local, __Attempts) ->
            FileRef = ?resolveFileRef(__Worker, __SessId, __FilePath),

            ?assertMatch(
                {ok, ExpJson__Local},
                opt_file_metadata:get_custom_metadata(__Worker, __SessId, FileRef, json, [], false),
                __Attempts
            )
    end)(Worker, SessId, FilePath, JSON, Attempts)
).

-define(assertRdfMetadata(Worker, SessId, FilePath, RDF, Attempts),
    (fun
        (__Worker, __SessId, __FilePath, <<>>, __Attempts) ->
            FileRef = ?resolveFileRef(__Worker, __SessId, __FilePath),

            ?assertMatch(
                ?ERR_POSIX(?ENODATA),
                opt_file_metadata:get_custom_metadata(__Worker, __SessId, FileRef, rdf, [], false),
                __Attempts
            );
        (__Worker, __SessId, __FilePath, ExpRdf__Local, __Attempts) ->
            FileRef = ?resolveFileRef(__Worker, __SessId, __FilePath),

            ?assertMatch(
                {ok, ExpRdf__Local},
                opt_file_metadata:get_custom_metadata(__Worker, __SessId, FileRef, rdf, [], false),
                __Attempts
            )
    end)(Worker, SessId, FilePath, RDF, Attempts)
).

-define(assertFile(Worker, SessionId, FilePath, ReadData, Xattrs, JSON, RDF),
    ?assertFile(Worker, SessionId, FilePath, ReadData, Xattrs, JSON, RDF, 1)).
-define(assertFile(Worker, SessionId, FilePath, ReadData, Xattrs, JSON, RDF, Attempts),
    begin
        ?assertInLs(Worker, SessionId, FilePath, Attempts),
        ?assertStat(Worker, SessionId, FilePath, Attempts),
        ?assertRead(Worker, SessionId, FilePath, 0, ReadData, Attempts),
        ?assertXattrs(Worker, SessionId, FilePath, Xattrs, Attempts),
        ?assertJsonMetadata(Worker, SessionId, FilePath, JSON, Attempts),
        ?assertRdfMetadata(Worker, SessionId, FilePath, RDF, Attempts)
    end).


-endif.
