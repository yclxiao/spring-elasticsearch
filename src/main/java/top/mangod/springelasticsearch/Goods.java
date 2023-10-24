package top.mangod.springelasticsearch;

import com.alibaba.fastjson2.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.math.BigDecimal;
import java.util.Date;

/**
 * ============ 创建mapping ============
 * PUT /goods
 * {
 *   "mappings": {
 *     "properties": {
 *       "brandName": {
 *         "type": "keyword"
 *       },
 *       "categoryName": {
 *         "type": "keyword"
 *       },
 *       "createTime": {
 *         "type": "date",
 *         "format": "yyyy-MM-dd HH:mm:ss"
 *       },
 *       "id": {
 *         "type": "keyword"
 *       },
 *       "price": {
 *         "type": "double"
 *       },
 *       "saleNum": {
 *         "type": "integer"
 *       },
 *       "status": {
 *         "type": "integer"
 *       },
 *       "stock": {
 *         "type": "integer"
 *       },
 *       "title": {
 *         "type": "text",
 *         "analyzer": "ik_max_word",
 *         "search_analyzer": "ik_smart"
 *       }
 *     }
 *   }
 * }
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Goods {

    /**
     * 商品编号
     */
    private Long id;

    /**
     * 商品标题
     */
    private String title;

    /**
     * 商品价格
     */
    private BigDecimal price;

    /**
     * 商品库存
     */
    private Integer stock;

    /**
     * 商品销售数量
     */
    private Integer saleNum;

    /**
     * 商品分类
     */
    private String categoryName;

    /**
     * 商品品牌
     */
    private String brandName;

    /**
     * 上下架状态
     */
    private Integer status;

    /**
     * 商品创建时间
     */
    @JSONField(format = "yyyy-MM-dd HH:mm:ss")
    private Date createTime;
}

