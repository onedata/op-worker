%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc @todo: Write me!
%%% @end
%%%-------------------------------------------------------------------
-author("Rafal Slota").

-record(document, {key, rev, value}).
-record(model_config, {name, size = 0, fields = [], defaults = {}, hooks = [], bucket}).

-record(some_record, {field1, field2, field3}).

-define(MODELS, [some_record]).