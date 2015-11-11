%%%-------------------------------------------------------------------
%%% @author Michal Zmuda
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% todo
%%% @end
%%%-------------------------------------------------------------------
-module(datastore_config_behaviour).
-author("Michal Zmuda").

%%--------------------------------------------------------------------
%% @doc
%% todo
%% @end
%%--------------------------------------------------------------------
-callback models() -> Models :: [model_behaviour:model_type()].

%%--------------------------------------------------------------------
%% @doc
%% todo
%% @end
%%--------------------------------------------------------------------
-callback global_caches() -> Models :: [model_behaviour:model_type()].

%%--------------------------------------------------------------------
%% @doc
%% todo
%% @end
%%--------------------------------------------------------------------
-callback local_caches() -> Models :: [model_behaviour:model_type()].
