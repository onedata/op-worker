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

-spec build_attributes_param_spec(middleware:scope(), file_attr_translator:attr_type(), binary()) ->
    middleware_sanitizer:param_spec().
build_attributes_param_spec(public, current = AttrType, Key) ->
    {any, build_parse_requested_attrs_fun(Key, AttrType, ?PUBLIC_ATTRS)};
build_attributes_param_spec(private, current = AttrType, Key) ->
    {any, build_parse_requested_attrs_fun(Key, AttrType, ?API_ATTRS)};
build_attributes_param_spec(public, deprecated = AttrType, Key) ->
    {any, build_parse_requested_attrs_fun(Key, AttrType, ?DEPRECATED_PUBLIC_ATTRS)};
build_attributes_param_spec(private, deprecated = AttrType, Key) ->
    {any, build_parse_requested_attrs_fun(Key, AttrType, ?DEPRECATED_ALL_ATTRS)}.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec build_parse_requested_attrs_fun(binary(), file_attr_translator:attr_type(), [file_attr:attribute()]) ->
    fun((binary() | [binary()]) -> {true, [atom()]} | no_return()).
build_parse_requested_attrs_fun(Key, AttrType, AllowedValues) ->
    % fixme call helper from file_attr level
    fun(Attributes) ->
        {TranslatedAttrs, Xattrs} = lists:foldl(fun
            (<<"xattr.", _/binary>> = Xattr, {AttrAcc, XattrAcc}) ->
                {AttrAcc, [Xattr | XattrAcc]};
            (Attr, {AttrAcc, XattrAcc}) ->
                try
                    TranslatedAttr = file_attr_translator:attr_name_from_json(AttrType, Attr),
                    true = lists:member(TranslatedAttr, AllowedValues),
                    {[TranslatedAttr | AttrAcc], XattrAcc}
                catch _:_ ->
                    AllowedValuesJson = [file_attr_translator:attr_name_to_json(AttrType, A) || A <- AllowedValues],
                    throw(?ERROR_BAD_VALUE_NOT_ALLOWED(Key, [<<"xattr.*">> | AllowedValuesJson]))
                end
        end, {[], []}, utils:ensure_list(Attributes)),
        {true, case Xattrs of
            [] -> TranslatedAttrs;
            _ -> [{xattrs, Xattrs} | TranslatedAttrs]
        end}
    end.
