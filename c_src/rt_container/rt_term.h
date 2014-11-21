/**
 * @file rt_term.h
 * @author Konrad Zemek
 * @copyright (C): 2014 ACK CYFRONET AGH
 * This software is released under the MIT license
 * cited in 'LICENSE.txt'.
 */

#ifndef RT_TERM_H
#define RT_TERM_H

#include "nifpp.h"

#include <memory>

namespace one {
namespace provider {

class rt_term {
    struct shared_data {
        shared_data(nifpp::TERM src_term)
            : env_{enif_alloc_env()}, term_{enif_make_copy(env_, src_term)}
        {
        }

        ~shared_data() { enif_free_env(env_); }

        ErlNifEnv *env_;
        ERL_NIF_TERM term_;
    };

public:
    rt_term() {}

    rt_term(nifpp::TERM src_term)
        : shared_data_{std::make_shared<shared_data>(src_term)}
    {
    }

    nifpp::TERM get(ErlNifEnv *dst_env) const
    {
        return nifpp::TERM{enif_make_copy(dst_env, shared_data_->term_)};
    }

    bool operator==(const rt_term &rhs) const
    {
        return shared_data_->term_ == rhs.shared_data_->term_;
    }

    bool operator<(const rt_term &rhs) const
    {
        return shared_data_->term_ < rhs.shared_data_->term_;
    }

private:
    std::shared_ptr<const shared_data> shared_data_;
};

} // namespace provider
} // namespace one

#endif // RT_TERM_H