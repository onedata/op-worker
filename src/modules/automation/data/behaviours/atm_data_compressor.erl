%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module defines `atm_data_compressor` interface - an object which can be
%%% used for compressing (before saving in store) and expanding (when retrieving
%%% from store) values of given type.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_data_compressor).
-author("Michal Stanisz").


%%%===================================================================
%%% Callbacks
%%%===================================================================


-callback compress(atm_value:expanded()) -> atm_value:compressed() | no_return().


-callback expand(atm_workflow_execution_auth:record(), atm_value:compressed()) ->
    {ok, atm_value:expanded()} | {error, term()}.
