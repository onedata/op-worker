%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module includes utility functions used in gui modules.
%%% @end
%%%-------------------------------------------------------------------
-module(op_gui_utils).
-author("Lukasz Opiola").

-include("proto/common/credentials.hrl").
-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([ids_to_association/2, association_to_ids/1]).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates an associative ID from two IDs which can be easily decoupled later.
%% @end
%%--------------------------------------------------------------------
-spec ids_to_association(FirstId :: binary(), SecondId :: binary()) -> binary().
ids_to_association(FirstId, SecondId) ->
    <<FirstId/binary, "|", SecondId/binary>>.


%%--------------------------------------------------------------------
%% @doc
%% Decouples an associative ID into two separate IDs.
%% @end
%%--------------------------------------------------------------------
-spec association_to_ids(AssocId :: binary()) -> {binary(), binary()}.
association_to_ids(AssocId) ->
    [FirstId, SecondId] = binary:split(AssocId, <<"|">>, [global]),
    {FirstId, SecondId}.