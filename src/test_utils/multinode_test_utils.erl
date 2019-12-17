%%%-------------------------------------------------------------------
%%% @author Michał Wrzeszcz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains helper functions for manual performance tests in multi-node environment.
%%% @end
%%%-------------------------------------------------------------------
-module(multinode_test_utils).
-author("Michał Wrzeszcz").

%% API
-export([gen_key/0, fslogic_ref_by_context_guid/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Generates random key handled on local node.
%% To be called by datastore_utils:gen_key/0
%% @end
%%--------------------------------------------------------------------
-spec gen_key() -> datastore:key().
gen_key() ->
    gen_key_node(node()).

%%--------------------------------------------------------------------
%% @doc
%% Generates ref to fslogic worker to be used by router.
%% To be called by router:fslogic_ref_by_context_guid/1
%% @end
%%--------------------------------------------------------------------
-spec fslogic_ref_by_context_guid(file_id:file_guid()) -> module() | {id, module(), datastore:key()}.
fslogic_ref_by_context_guid(ContextGuid) ->
    case fslogic_uuid:is_space_dir_guid(ContextGuid) of
        true -> fslogic_worker;
        _ -> {id, fslogic_worker, file_id:guid_to_uuid(ContextGuid)}
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Function generating key connected with particular node (for tests only).
%% @end
%%--------------------------------------------------------------------
-spec gen_key_node(node()) -> datastore:key().
gen_key_node(TargetNode) ->
    Key = consistent_hashing:gen_hashing_key(),
    case consistent_hashing:get_node(Key) of
        TargetNode ->
            datastore_utils:gen_key(Key);
        _ ->
            gen_key_node(TargetNode)
    end.