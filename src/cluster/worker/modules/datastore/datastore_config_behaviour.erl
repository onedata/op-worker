%%%-------------------------------------------------------------------
%%% @author Michal Zmuda
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% His behaviour is used to inject datastore config related to use of
%%% specific models.
%%% Module implementing it should be named datastore_config_plugin.
%%% This is required element.
%%% @end
%%%-------------------------------------------------------------------
-module(datastore_config_behaviour).
-author("Michal Zmuda").

%%--------------------------------------------------------------------
%% @doc
%% List of models to be used. Should not contain modules used by default.
%% @end
%%--------------------------------------------------------------------
-callback models() -> Models :: [model_behaviour:model_type()].

%%--------------------------------------------------------------------
%% @doc
%% List of models to be cached globally. Should not contain modules
%% cached by default.
%% @end
%%--------------------------------------------------------------------
-callback global_caches() -> Models :: [model_behaviour:model_type()].

%%--------------------------------------------------------------------
%% @doc
%% List of models to be cached locally. Should not contain modules
%% cached by default.
%% @end
%%--------------------------------------------------------------------
-callback local_caches() -> Models :: [model_behaviour:model_type()].
