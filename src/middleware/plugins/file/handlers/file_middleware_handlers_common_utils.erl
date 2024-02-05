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

-spec build_attributes_param_spec(middleware:scope(), file_attr_translator:attr_type() | deprecated_recursive, binary()) ->
    middleware_sanitizer:param_spec().
build_attributes_param_spec(public, current = AttrType, Key) ->
    {any, build_parse_requested_attrs_fun(Key, AttrType, ?PUBLIC_API_ATTRS)};
build_attributes_param_spec(private, current = AttrType, Key) ->
    {any, build_parse_requested_attrs_fun(Key, AttrType, ?API_ATTRS)};
build_attributes_param_spec(public, deprecated = AttrType, Key) ->
    {any, build_parse_requested_attrs_fun(Key, AttrType, ?DEPRECATED_PUBLIC_ATTRS)};
build_attributes_param_spec(private, deprecated = AttrType, Key) ->
    {any, build_parse_requested_attrs_fun(Key, AttrType, ?DEPRECATED_ALL_ATTRS)};
build_attributes_param_spec(private, deprecated_recursive, Key) ->
    {any, build_parse_requested_attrs_fun(Key, deprecated, [path | ?DEPRECATED_ALL_ATTRS])}.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec build_parse_requested_attrs_fun(binary(), file_attr_translator:attr_type(), [file_attr:attribute()]) ->
    fun((binary() | [binary()]) -> {true, [atom()]} | no_return()).
build_parse_requested_attrs_fun(Key, AttrType, AllowedValues) ->
    fun(Attributes) ->
        case file_attr_translator:sanitize_requested_attrs(Attributes, AttrType, AllowedValues) of
            {ok, FinalAttrs} -> {true, FinalAttrs};
            {error, AllowedValuesJson} -> throw(?ERROR_BAD_VALUE_NOT_ALLOWED(Key, AllowedValuesJson))
        end
    end.
