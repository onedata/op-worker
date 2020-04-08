%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Common definitions and macros for logical file manager.
%%% @end
%%%-------------------------------------------------------------------
-author("Bartosz Walkowicz").

-ifndef(LFM_HRL).
-define(LFM_HRL, 1).

-define(check(__FunctionCall), lfm:check_result(__FunctionCall)).

-define(PRIVATE_BASIC_ATTRIBUTES, [
    <<"file_id">>, <<"name">>, <<"mode">>,
    <<"storage_user_id">>, <<"storage_group_id">>,
    <<"atime">>, <<"mtime">>, <<"ctime">>,
    <<"type">>, <<"size">>, <<"shares">>,
    <<"provider_id">>, <<"owner_id">>
]).
-define(PUBLIC_BASIC_ATTRIBUTES, [
    <<"file_id">>, <<"name">>, <<"mode">>,
    <<"atime">>, <<"mtime">>, <<"ctime">>,
    <<"type">>, <<"size">>, <<"shares">>
]).
-define(ALL_BASIC_ATTRIBUTES, [
    <<"file_id">>, <<"name">>, <<"mode">>,
    <<"storage_user_id">>, <<"storage_group_id">>,
    <<"atime">>, <<"mtime">>, <<"ctime">>,
    <<"type">>, <<"size">>, <<"shares">>,
    <<"provider_id">>, <<"owner_id">>
]).

-endif.
