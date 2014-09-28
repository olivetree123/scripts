#coding:utf-8

import spynner
#例子1
#https://code.google.com/p/spynner/
browser = spynner.Browser()
browser.load("http://www.wordreference.com")
browser.runjs("console.log('I can run Javascript!')")
browser.runjs("_jQuery('div').css('border', 'solid red')") # and jQuery!
browser.select("#esen")
browser.fill("input[name=enit]", "hola")
browser.click("input[name=b]")
browser.wait_page_load()
print browser.url, len(browser.html)
browser.close()

'''
#例子2
#http://www.cnblogs.com/caroar/archive/2013/05/10/3070847.html
browser = spynner.Browser()
    # 设置代理
    #browser.set_proxy('http://host:port')
    browser.show()
    try:
        browser.load(url='http://duckduckgo.com', load_timeout=120, tries=1)
    except spynner.SpynnerTimeout:
        print 'Timeout.'
    else:
        # 输入搜索关键字
        browser.wk_fill('input[id="search_form_input_homepage"]', 'something')
        # 点击搜索按钮，并等待页面加载完毕
        browser.wk_click('input[id="search_button_homepage"]', wait_load=True)
        # 获取页面的HTML
        html = browser.html
        if html:
            html = html.encode('utf-8')
            open('search_results.html', 'w').write(html)
    browser.close()
'''