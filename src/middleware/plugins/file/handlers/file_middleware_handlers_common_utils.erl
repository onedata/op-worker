%%%-------------------------------------------------------------------
%%% @author Michał Stanisz
%%% @copyright (C) 2023 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module contains utility functions used across file_middleware_plugin_* modules.
%%% @end
%%%-------------------------------------------------------------------
-module(file_middleware_handlers_common_utils).
-author("Michal Stanisz").


-include("modules/fslogic/file_attr.hrl").
-include_lib("ctool/include/errors.hrl").


-export([build_attributes_param_spec/3]).

%%%===================================================================
%%% API
%%%===================================================================

-spec build_attributes_param_spec(middleware:scope(), onedata_file:attr_generation() | deprecated_recursive, binary()) ->
    middleware_sanitizer:param_spec().
build_attributes_param_spec(public, current = AttrGeneration, Key) ->
    {any, build_parse_requested_attrs_fun(Key, AttrGeneration, ?PUBLIC_API_FILE_ATTRS)};
build_attributes_param_spec(private, current = AttrGeneration, Key) ->
    {any, build_parse_requested_attrs_fun(Key, AttrGeneration, ?API_FILE_ATTRS)};
build_attributes_param_spec(public, deprecated = AttrGeneration, Key) ->
    {any, build_parse_requested_attrs_fun(Key, AttrGeneration, ?DEPRECATED_PUBLIC_FILE_ATTRS)};
build_attributes_param_spec(private, deprecated = AttrGeneration, Key) ->
    {any, build_parse_requested_attrs_fun(Key, AttrGeneration, ?DEPRECATED_ALL_FILE_ATTRS)};
build_attributes_param_spec(private, deprecated_recursive, Key) ->
    {any, build_parse_requested_attrs_fun(Key, deprecated, [path | ?DEPRECATED_ALL_FILE_ATTRS])}.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec build_parse_requested_attrs_fun(binary(), onedata_file:attr_generation(), [onedata_file:attr_name()]) ->
    fun((binary() | [binary()]) -> {true, [onedata_file:attr_name()]} | no_return()).
build_parse_requested_attrs_fun(Key, AttrGeneration, AllowedValues) ->
    fun(AttrNames) ->
        {true, onedata_file:sanitize_attr_names(Key, AttrNames, AttrGeneration, AllowedValues)}
    end.
