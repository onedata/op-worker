%%%-------------------------------------------------------------------
%%% @author Łukasz Opioła
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module encapsulates all common logic available in provider.
%%% It is used to process entity requests is a standardized way, i.e.:
%%%     # checks existence of given entity
%%%     # checks authorization of client to perform certain action
%%%     # checks validity of data provided in the request
%%%     # handles all errors in a uniform way
%%% @end
%%%-------------------------------------------------------------------
-module(op_logic).
-author("Łukasz Opioła").
-author("Bartosz Walkowicz").

%% API
-export([handle/1]).


handle(_) -> ok.
