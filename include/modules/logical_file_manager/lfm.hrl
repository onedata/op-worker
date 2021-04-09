%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019-2021 ACK CYFRONET AGH
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
    <<"file_id">>, <<"parent_id">>, <<"name">>, <<"mode">>,
    <<"storage_user_id">>, <<"storage_group_id">>,
    <<"atime">>, <<"mtime">>, <<"ctime">>,
    <<"type">>, <<"size">>, <<"shares">>,
    <<"provider_id">>, <<"owner_id">>, <<"hardlinks_count">>
]).
-define(PUBLIC_BASIC_ATTRIBUTES, [
    <<"file_id">>, <<"parent_id">>, <<"name">>, <<"mode">>,
    <<"atime">>, <<"mtime">>, <<"ctime">>,
    <<"type">>, <<"size">>, <<"shares">>
]).

-define(GUID_KEY(__GUID), {guid, __GUID}).
-define(DIRECT_GUID_KEY(__GUID), {direct_guid, __GUID}).
-define(INDIRECT_GUID_KEY(__GUID), {indirect_guid, __GUID}).

-define(PATH_KEY(__PATH), {path, __PATH}).
-define(DIRECT_PATH_KEY(__PATH), {direct_path, __PATH}).
-define(INDIRECT_PATH_KEY(__PATH), {indirect_path, __PATH}).

-endif.
