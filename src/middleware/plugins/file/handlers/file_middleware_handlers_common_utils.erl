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


-export([build_attributes_param_spec/1]).

%%%===================================================================
%%% API
%%%===================================================================

-spec build_attributes_param_spec(middleware:scope()) -> middleware_sanitizer:param_spec().
build_attributes_param_spec(public) ->
    {any, build_parse_requested_attrs_fun(?PUBLIC_ATTRS)};
build_attributes_param_spec(private) ->
    {any, build_parse_requested_attrs_fun(?API_ATTRS)}.


%% @private
-spec build_parse_requested_attrs_fun([file_attr:attributes()]) ->
    fun((binary() | [binary()]) -> {true, [atom()]} | no_return()).
build_parse_requested_attrs_fun(AllowedValues) ->
    fun(Attributes) ->
        {TranslatedAttrs, Xattrs} = lists:foldl(fun
            (<<"xattr.", _/binary>> = Xattr, {AttrAcc, XattrAcc}) ->
                {AttrAcc, [Xattr | XattrAcc]};
            (Attr, {AttrAcc, XattrAcc}) ->
                try
                    TranslatedAttr = file_attr_translator:attr_name_from_json(Attr),
                    true = lists:member(TranslatedAttr, AllowedValues),
                    {[TranslatedAttr | AttrAcc], XattrAcc}
                catch _:_ ->
                    AllowedValuesJson = [file_attr_translator:attr_name_to_json(A) || A <- AllowedValues],
                    throw(?ERROR_BAD_VALUE_NOT_ALLOWED(<<"attributes">>, AllowedValuesJson ++ [<<"xattr.*">>]))
                end
        end, {[], []}, utils:ensure_list(Attributes)),
        {true, case Xattrs of
            [] -> TranslatedAttrs;
            _ -> [{xattrs, Xattrs} | TranslatedAttrs]
        end}
    end.
