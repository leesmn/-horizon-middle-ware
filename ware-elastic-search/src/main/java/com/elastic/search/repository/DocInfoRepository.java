package com.elastic.search.repository;

import com.elastic.search.model.DocInfo;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;

/**
 * @author lis
 * @description:
 * @date 2019/08/01
 **/
public interface DocInfoRepository extends ElasticsearchRepository<DocInfo,Long> {
}
