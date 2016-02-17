%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Module for invalidating blocks of file replicas
%%% @end
%%%--------------------------------------------------------------------
-module(replica_invalidator).
-author("Tomasz Lichon").

-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include("proto/oneclient/common_messages.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([invalidate_changes/2, invalidate/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Invalidate given list of changes in blocks of file_location.
%% @end
%%--------------------------------------------------------------------
-spec invalidate_changes(file_location:doc(), Changes :: list()) -> ok.
invalidate_changes(Doc, []) ->
    {ok, _} = file_location:save(Doc#document{rev = undefined}); %todo do we need undefined rev here?
invalidate_changes(Doc = #document{value = Loc = #file_location{blocks = OldBlocks}}, [Blocks | Rest]) ->
    NewBlocks = fslogic_blocks:invalidate(OldBlocks, Blocks),
    NewBlocks1 = fslogic_blocks:consolidate(NewBlocks),
    invalidate_changes(Doc#document{value = Loc#file_location{blocks = NewBlocks1}}, Rest).

%%--------------------------------------------------------------------
%% @doc
%% Invalidates given blocks in given locations. File size is also updated.
%% @end
%%--------------------------------------------------------------------
-spec invalidate(datastore:document() | [datastore:document()], Blocks :: fslogic_blocks:blocks()) ->
    ok | no_return().
invalidate([Location | T], Blocks) ->
    ok = invalidate(Location, Blocks),
    ok = invalidate(T, Blocks);
invalidate(_, []) ->
    ok;
invalidate(Doc = #document{value = Loc = #file_location{blocks = OldBlocks}}, Blocks) ->
    ?debug("OldBlocks invalidate ~p, new ~p", [OldBlocks, Blocks]),
    NewBlocks = fslogic_blocks:invalidate(OldBlocks, Blocks),
    ?debug("NewBlocks invalidate ~p", [NewBlocks]),
    NewBlocks1 = fslogic_blocks:consolidate(NewBlocks),
    ?debug("NewBlocks1 invalidate ~p", [NewBlocks1]),
    {ok, _} = file_location:save(Doc#document{rev = undefined, value = Loc#file_location{blocks = NewBlocks1}}),
    ok;
invalidate([], _) ->
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================