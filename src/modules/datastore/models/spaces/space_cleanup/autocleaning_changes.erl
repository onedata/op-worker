%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% Handles changes of autocleaning document
%%% @end
%%%-------------------------------------------------------------------
-module(autocleaning_changes).
-author("Jakub Kudzia").


-include("proto/oneprovider/provider_messages.hrl").
-include("modules/datastore/datastore_models.hrl").

%% API
-export([handle/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%-------------------------------------------------------------------
%% @doc
%% Handles change on autocleaning document
%% @end
%%-------------------------------------------------------------------
-spec handle(autocleaning:doc()) -> ok.
handle(#document{
    value = #autocleaning{
        space_id = SpaceId,
        status = active,
        files_to_process = FilesToProcess,
        files_processed = FilesToProcess
    }
}) ->
    autocleaning_controller:stop(SpaceId);
handle(_) ->
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

